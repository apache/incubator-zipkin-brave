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

import brave.Span;
import brave.Tracing;
import brave.messaging.MessagingTracing;
import brave.propagation.B3SingleFormat;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import java.util.Iterator;
import java.util.List;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import static brave.kafka.clients.KafkaPropagation.B3_SINGLE_TEST_HEADERS;
import static brave.kafka.clients.KafkaPropagation.HEADERS_GETTER;
import static brave.kafka.clients.KafkaPropagation.HEADERS_SETTER;
import static brave.kafka.clients.KafkaPropagation.TEST_CONTEXT;
import static brave.propagation.B3SingleFormat.writeB3SingleFormatWithoutParentIdAsBytes;

/** Use this class to decorate your Kafka consumer / producer and enable Tracing. */
public final class KafkaTracing {

  static final String PROTOCOL = "kafka";
  static final String PRODUCER_OPERATION = "send";
  static final String CONSUMER_OPERATION = "poll";

  public static KafkaTracing create(Tracing tracing) {
    return new Builder(tracing).build();
  }

  public static Builder newBuilder(Tracing tracing) {
    return new Builder(tracing);
  }

  public static final class Builder {
    final MessagingTracing msgTracing;
    String remoteServiceName = "kafka";
    boolean writeB3SingleFormat;

    Builder(Tracing tracing) {
      if (tracing == null) throw new NullPointerException("tracing == null");
      this.msgTracing = MessagingTracing.create(tracing);
    }

    Builder(MessagingTracing msgTracing) {
      if (msgTracing == null) throw new NullPointerException("msgTracing == null");
      this.msgTracing = msgTracing;
    }

    /**
     * The remote service name that describes the broker in the dependency graph. Defaults to
     * "kafka"
     */
    public Builder remoteServiceName(String remoteServiceName) {
      this.remoteServiceName = remoteServiceName;
      return this;
    }

    /**
     * When true, only writes a single {@link B3SingleFormat b3 header} for outbound propagation.
     *
     * <p>Use this to reduce overhead. Note: normal {@link Tracing#propagation()} is used to parse
     * incoming headers. The implementation must be able to read "b3" headers.
     */
    public Builder writeB3SingleFormat(boolean writeB3SingleFormat) {
      this.writeB3SingleFormat = writeB3SingleFormat;
      return this;
    }

    public KafkaTracing build() {
      return new KafkaTracing(this);
    }
  }

  final MessagingTracing msgTracing;
  final String remoteServiceName;
  final List<String> propagationKeys;

  boolean singleFormat;

  KafkaTracing(Builder builder) { // intentionally hidden constructor
    this.msgTracing = builder.msgTracing;
    this.remoteServiceName = builder.remoteServiceName;
    this.propagationKeys = msgTracing.tracing().propagation().keys();
    final Extractor<Headers> extractor =
      msgTracing.tracing().propagation().extractor(HEADERS_GETTER);
    List<String> keyList = msgTracing.tracing().propagation().keys();
    singleFormat = false;
    if (builder.writeB3SingleFormat || keyList.equals(Propagation.B3_SINGLE_STRING.keys())) {
      TraceContext testExtraction = extractor.extract(B3_SINGLE_TEST_HEADERS).context();
      if (!TEST_CONTEXT.equals(testExtraction)) {
        throw new IllegalArgumentException(
          "KafkaTracing.Builder.writeB3SingleFormat set, but Tracing.Builder.propagationFactory cannot parse this format!");
      }
      singleFormat = true;
    }
  }

  <K, V> Extractor<ProducerRecord<K, V>> producerRecordExtractor() {
    return msgTracing.tracing()
      .propagation()
      .extractor((record, key) -> HEADERS_GETTER.get(record.headers(), key));
  }

  <K, V> Injector<ProducerRecord<K, V>> producerRecordInjector() {
    return singleFormat ?
      new Injector<ProducerRecord<K, V>>() {
        @Override public void inject(TraceContext traceContext, ProducerRecord<K, V> carrier) {
          carrier.headers().add("b3", writeB3SingleFormatWithoutParentIdAsBytes(traceContext));
        }

        @Override public String toString() {
          return "Headers::add(\"b3\",singleHeaderFormatWithoutParent)";
        }
      }
      : msgTracing.tracing().propagation().injector((record, key, value) -> {
        HEADERS_SETTER.put(record.headers(), key, value);
      });
  }

  <K, V> Extractor<ConsumerRecord<K, V>> consumerRecordExtractor() {
    return msgTracing.tracing()
      .propagation()
      .extractor((record, key) -> HEADERS_GETTER.get(record.headers(), key));
  }

  <K, V> Injector<ConsumerRecord<K, V>> consumerRecordInjector() {
    return singleFormat ?
      new Injector<ConsumerRecord<K, V>>() {
        @Override public void inject(TraceContext traceContext, ConsumerRecord<K, V> carrier) {
          carrier.headers().add("b3", writeB3SingleFormatWithoutParentIdAsBytes(traceContext));
        }

        @Override public String toString() {
          return "Headers::add(\"b3\",singleHeaderFormatWithoutParent)";
        }
      }
      : msgTracing.tracing().propagation().injector((record, key, value) -> {
        HEADERS_SETTER.put(record.headers(), key, value);
      });
  }

  /**
   * Extracts or creates a {@link Span.Kind#CONSUMER} span for each message received. This span is
   * injected onto each message so it becomes the parent when a processor later calls {@link
   * #nextSpan(ConsumerRecord)}.
   */
  public <K, V> Consumer<K, V> consumer(Consumer<K, V> consumer) {
    if (consumer == null) throw new NullPointerException("consumer == null");
    return new TracingConsumer<>(consumer, this);
  }

  /** Starts and propagates {@link Span.Kind#PRODUCER} span for each message sent. */
  public <K, V> Producer<K, V> producer(Producer<K, V> producer) {
    if (producer == null) throw new NullPointerException("producer == null");
    return new TracingProducer<>(producer, this);
  }

  /**
   * Use this to create a span for processing the given record. Note: the result has no name and is
   * not started.
   *
   * <p>This creates a child from identifiers extracted from the record headers, or a new span if
   * one couldn't be extracted.
   */
  public <K, V> Span nextSpan(ConsumerRecord<K, V> record) {
    final TracingConsumer.KafkaConsumerAdapter<K, V> adapter =
      TracingConsumer.KafkaConsumerAdapter.create(this);
    return msgTracing.nextSpan(adapter, adapter, consumerRecordExtractor(), record, record);
  }

  <Record> String channelTagKey(Record record) {
    return String.format("%s.topic", PROTOCOL);
  }

  String recordKey(Object key) {
    if (key instanceof String && !"".equals(key)) {
      return key.toString();
    }
    return null;
  }

  String identifierTagKey() {
    return String.format("%s.key", PROTOCOL);
  }

  // BRAVE6: consider a messaging variant of extraction which clears headers as they are read.
  // this could prevent having to go back and clear them later. Another option is to encourage,
  // then special-case single header propagation. When there's only 1 propagation key, you don't
  // need to do a loop!
  void clearHeaders(Headers headers) {
    // Headers::remove creates and consumes an iterator each time. This does one loop instead.
    for (Iterator<Header> i = headers.iterator(); i.hasNext(); ) {
      Header next = i.next();
      if (propagationKeys.contains(next.key())) i.remove();
    }
  }
}
