/*
 * Copyright 2013-2019 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package brave.kafka.clients;

import brave.Tracing;
import brave.internal.HexCodec;
import brave.propagation.Propagation;
import brave.propagation.SamplingFlags;
import brave.propagation.StrictScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.propagation.TraceIdContext;
import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import zipkin2.DependencyLink;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;

import static brave.kafka.clients.KafkaTags.KAFKA_TOPIC_TAG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

public class ITKafkaTracing {

  String TEST_KEY = "foo";
  String TEST_VALUE = "bar";

  /**
   * See brave.http.ITHttp for rationale on using a concurrent blocking queue eventhough some calls,
   * like consumer operations, happen on the main thread.
   */
  BlockingQueue<Span> consumerSpans = new LinkedBlockingQueue<>();
  BlockingQueue<Span> producerSpans = new LinkedBlockingQueue<>();

  KafkaTracing consumerTracing = KafkaTracing.create(Tracing.newBuilder()
    .localServiceName("consumer")
    .currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder()
      .addScopeDecorator(StrictScopeDecorator.create())
      .build())
    .spanReporter(consumerSpans::add)
    .build());
  KafkaTracing producerTracing = KafkaTracing.create(Tracing.newBuilder()
    .localServiceName("producer")
    .currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder()
      .addScopeDecorator(StrictScopeDecorator.create())
      .build())
    .spanReporter(producerSpans::add)
    .build());

  @ClassRule
  public static KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create());
  @Rule
  public TestName testName = new TestName();

  Producer<String, String> producer;
  Consumer<String, String> consumer;

  // See brave.http.ITHttp for rationale on polling after tests complete
  @Rule public TestRule assertSpansEmpty = new TestWatcher() {
    // only check success path to avoid masking assertion errors or exceptions
    @Override protected void succeeded(Description description) {
      try {
        assertThat(producerSpans.poll(100, TimeUnit.MILLISECONDS))
          .withFailMessage("Producer span remaining in queue. Check for redundant reporting")
          .isNull();
        assertThat(consumerSpans.poll(100, TimeUnit.MILLISECONDS))
          .withFailMessage("Consumer span remaining in queue. Check for redundant reporting")
          .isNull();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  };

  @After
  public void close() {
    if (producer != null) producer.close();
    if (consumer != null) consumer.close();
    Tracing current = Tracing.current();
    if (current != null) current.close();
  }

  @Test
  public void poll_creates_one_consumer_span_per_extracted_context() throws Exception {
    String topic1 = testName.getMethodName() + "1";
    String topic2 = testName.getMethodName() + "2";

    producer = createTracingProducer();
    consumer = createTracingConsumer(topic1, topic2);

    producer.send(new ProducerRecord<>(topic1, TEST_KEY, TEST_VALUE)).get();
    producer.send(new ProducerRecord<>(topic2, TEST_KEY, TEST_VALUE)).get();

    ConsumerRecords<String, String> records = consumer.poll(10000);

    assertThat(records).hasSize(2);
    Span producerSpan1 = takeProducerSpan(), producerSpan2 = takeProducerSpan();
    Span consumerSpan1 = takeConsumerSpan(), consumerSpan2 = takeConsumerSpan();

    // Check to see the trace is continued between the producer and the consumer
    // we don't know the order the spans will come in. Correlate with the tag instead.
    String firstTopic = producerSpan1.tags().get(KAFKA_TOPIC_TAG);
    if (firstTopic.equals(consumerSpan1.tags().get(KAFKA_TOPIC_TAG))) {
      assertThat(producerSpan1.traceId())
        .isEqualTo(consumerSpan1.traceId());
      assertThat(producerSpan2.traceId())
        .isEqualTo(consumerSpan2.traceId());
    } else {
      assertThat(producerSpan1.traceId())
        .isEqualTo(consumerSpan2.traceId());
      assertThat(producerSpan2.traceId())
        .isEqualTo(consumerSpan1.traceId());
    }
  }

  @Test
  public void poll_creates_one_consumer_span_per_topic() throws Exception {
    String topic1 = testName.getMethodName() + "1";
    String topic2 = testName.getMethodName() + "2";

    producer = kafkaRule.helper().createStringProducer(); // not traced
    consumer = createTracingConsumer(topic1, topic2);

    for (int i = 0; i < 5; i++) {
      producer.send(new ProducerRecord<>(topic1, TEST_KEY, TEST_VALUE)).get();
      producer.send(new ProducerRecord<>(topic2, TEST_KEY, TEST_VALUE)).get();
    }

    ConsumerRecords<String, String> records = consumer.poll(10000);

    assertThat(records).hasSize(10);
    takeConsumerSpan();
    takeConsumerSpan();
    // producerSpans empty as not traced
  }

  @Test
  public void creates_dependency_links() throws Exception {
    producer = createTracingProducer();
    consumer = createTracingConsumer();

    producer.send(new ProducerRecord<>(testName.getMethodName(), TEST_KEY, TEST_VALUE)).get();

    consumer.poll(10000);

    List<Span> allSpans = new ArrayList<>();
    allSpans.add(takeConsumerSpan());
    allSpans.add(takeProducerSpan());

    List<DependencyLink> links = new DependencyLinker().putTrace(allSpans).link();
    assertThat(links).extracting("parent", "child").containsExactly(
      tuple("producer", "kafka"),
      tuple("kafka", "consumer")
    );
  }

  @Test
  public void nextSpan_makes_child() throws Exception {
    producer = createTracingProducer();
    consumer = createTracingConsumer();

    producer.send(new ProducerRecord<>(testName.getMethodName(), TEST_KEY, TEST_VALUE)).get();

    ConsumerRecords<String, String> records = consumer.poll(10000);

    assertThat(records).hasSize(1);
    Span producerSpan = takeProducerSpan();
    Span consumerSpan = takeConsumerSpan();

    for (ConsumerRecord<String, String> record : records) {
      brave.Span processor = consumerTracing.nextSpan(record);

      assertThat(consumerSpan.tags())
        .containsEntry(KAFKA_TOPIC_TAG, record.topic());

      assertThat(processor.context().traceIdString()).isEqualTo(consumerSpan.traceId());
      assertThat(processor.context().parentIdString()).isEqualTo(consumerSpan.id());

      processor.start().name("processor").finish();

      // The processor doesn't taint the consumer span which has already finished
      Span processorSpan = takeConsumerSpan();
      assertThat(processorSpan.id())
        .isNotEqualTo(consumerSpan.id());
    }
  }

  static class TraceIdOnlyPropagation<K> implements Propagation<K> {
    final K key;

    TraceIdOnlyPropagation(Propagation.KeyFactory<K> keyFactory) {
      key = keyFactory.create("x-b3-traceid");
    }

    @Override public List<K> keys() {
      return Collections.singletonList(key);
    }

    @Override public <C> TraceContext.Injector<C> injector(Setter<C, K> setter) {
      return (traceContext, carrier) -> setter.put(carrier, key, traceContext.traceIdString());
    }

    @Override public <C> TraceContext.Extractor<C> extractor(Getter<C, K> getter) {
      return carrier -> {
        String result = getter.get(carrier, key);
        if (result == null) return TraceContextOrSamplingFlags.create(SamplingFlags.EMPTY);
        return TraceContextOrSamplingFlags.create(TraceIdContext.newBuilder()
          .traceId(HexCodec.lowerHexToUnsignedLong(result))
          .build());
      };
    }
  }

  @Test
  public void continues_a_trace_when_only_trace_id_propagated() throws Exception {
    consumerTracing = KafkaTracing.create(Tracing.newBuilder()
      .spanReporter(consumerSpans::add)
      .propagationFactory(new Propagation.Factory() {
        @Override public <K> Propagation<K> create(Propagation.KeyFactory<K> keyFactory) {
          return new TraceIdOnlyPropagation<>(keyFactory);
        }
      })
      .build());
    producerTracing = KafkaTracing.create(Tracing.newBuilder()
      .spanReporter(producerSpans::add)
      .propagationFactory(new Propagation.Factory() {
        @Override public <K> Propagation<K> create(Propagation.KeyFactory<K> keyFactory) {
          return new TraceIdOnlyPropagation<>(keyFactory);
        }
      })
      .build());

    producer = createTracingProducer();
    consumer = createTracingConsumer();

    producer.send(new ProducerRecord<>(testName.getMethodName(), TEST_KEY, TEST_VALUE)).get();

    // intentionally using deprecated method as we are checking the same class in an invoker test
    // under src/it. If we want to explicitly tests the Duration arg, we will have to subclass.
    ConsumerRecords<String, String> records = consumer.poll(10_000L);

    assertThat(records).hasSize(1);
    Span producerSpan = takeProducerSpan();
    Span consumerSpan = takeConsumerSpan();

    assertThat(producerSpan.traceId())
      .isEqualTo(consumerSpan.traceId());

    for (ConsumerRecord<String, String> record : records) {
      TraceContext forProcessor = consumerTracing.nextSpan(record).context();

      assertThat(forProcessor.traceIdString()).isEqualTo(consumerSpan.traceId());
    }
  }

  Consumer<String, String> createTracingConsumer(String... topics) {
    if (topics.length == 0) topics = new String[] {testName.getMethodName()};
    KafkaConsumer<String, String> consumer = kafkaRule.helper().createStringConsumer();
    List<TopicPartition> assignments = new ArrayList<>();
    for (String topic : topics) {
      assignments.add(new TopicPartition(topic, 0));
    }
    consumer.assign(assignments);
    return consumerTracing.consumer(consumer);
  }

  Producer<String, String> createTracingProducer() {
    KafkaProducer<String, String> producer = kafkaRule.helper().createStringProducer();
    return producerTracing.producer(producer);
  }

  /** Call this to block until a span was reported */
  Span takeProducerSpan() throws InterruptedException {
    Span result = producerSpans.poll(3, TimeUnit.SECONDS);
    assertThat(result)
      .withFailMessage("Producer span was not reported")
      .isNotNull();
    // ensure the span finished
    assertThat(result.durationAsLong()).isPositive();
    return result;
  }

  /** Call this to block until a span was reported */
  Span takeConsumerSpan() throws InterruptedException {
    Span result = consumerSpans.poll(3, TimeUnit.SECONDS);
    assertThat(result)
      .withFailMessage("Consumer span was not reported")
      .isNotNull();
    // ensure the span finished
    assertThat(result.durationAsLong()).isGreaterThanOrEqualTo(0);
    return result;
  }
}
