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
import brave.Tracer;
import brave.internal.Nullable;
import brave.messaging.ChannelAdapter;
import brave.messaging.MessageProducerAdapter;
import brave.messaging.MessagingProducerHandler;
import brave.propagation.CurrentTraceContext;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

final class TracingProducer<K, V>
  extends MessagingProducerHandler<Producer<K, V>, ProducerRecord<K, V>, ProducerRecord<K, V>>
  implements Producer<K, V> {

  final KafkaTracing kafkaTracing;
  final CurrentTraceContext current;
  final Tracer tracer;
  @Nullable final String remoteServiceName;

  TracingProducer(Producer<K, V> delegate, KafkaTracing kafkaTracing) {
    super(
      delegate,
      kafkaTracing.msgTracing,
      KafkaProducerAdapter.create(kafkaTracing),
      KafkaProducerAdapter.create(kafkaTracing),
      kafkaTracing.producerRecordExtractor(),
      kafkaTracing.producerRecordInjector());
    this.kafkaTracing = kafkaTracing;
    this.current = kafkaTracing.msgTracing.tracing().currentTraceContext();
    this.tracer = kafkaTracing.msgTracing.tracing().tracer();
    this.remoteServiceName = kafkaTracing.remoteServiceName;
  }

  @Override public void initTransactions() {
    delegate.initTransactions();
  }

  @Override public void beginTransaction() {
    delegate.beginTransaction();
  }

  @Override public void commitTransaction() {
    delegate.commitTransaction();
  }

  @Override public void abortTransaction() {
    delegate.abortTransaction();
  }

  /**
   * Send with a callback is always called for KafkaProducer. We do the same here to enable
   * tracing.
   */
  @Override public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return this.send(record, null);
  }

  /**
   * This wraps the send method to add tracing.
   *
   * <p>When there is no current span, this attempts to extract one from headers. This is possible
   * when a call to produce a message happens directly after a tracing consumer received a span. One
   * example scenario is Kafka Streams instrumentation.
   */
  // TODO: make b3single an option and then note how using this minimizes overhead
  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, @Nullable Callback callback) {
    Span span = handleProduce(record, record);

    try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
      return delegate.send(record, TracingCallback.create(callback, span, current));
    } catch (RuntimeException | Error e) {
      span.error(e).finish(); // finish as an exception means the callback won't finish the span
      throw e;
    }
  }

  @Override public void flush() {
    delegate.flush();
  }

  @Override public List<PartitionInfo> partitionsFor(String topic) {
    return delegate.partitionsFor(topic);
  }

  @Override public Map<MetricName, ? extends Metric> metrics() {
    return delegate.metrics();
  }

  @Override public void close() {
    delegate.close();
  }

  // Do not use @Override annotation to avoid compatibility on deprecated methods
  public void close(long timeout, TimeUnit unit) {
    delegate.close(timeout, unit);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.0
  public void close(Duration duration) {
    delegate.close(duration);
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
    String consumerGroupId) {
    delegate.sendOffsetsToTransaction(offsets, consumerGroupId);
  }

  /**
   * Decorates, then finishes a producer span. Allows tracing to record the duration between
   * batching for send and actual send.
   */
  static final class TracingCallback {
    static Callback create(@Nullable Callback delegate, Span span, CurrentTraceContext current) {
      if (span.isNoop()) return delegate; // save allocation overhead
      if (delegate == null) return new FinishSpan(span);
      return new DelegateAndFinishSpan(delegate, span, current);
    }

    static class FinishSpan implements Callback {
      final Span span;

      FinishSpan(Span span) {
        this.span = span;
      }

      @Override public void onCompletion(RecordMetadata metadata, @Nullable Exception exception) {
        if (exception != null) span.error(exception);
        span.finish();
      }
    }

    static final class DelegateAndFinishSpan extends FinishSpan {
      final Callback delegate;
      final CurrentTraceContext current;

      DelegateAndFinishSpan(Callback delegate, Span span, CurrentTraceContext current) {
        super(span);
        this.delegate = delegate;
        this.current = current;
      }

      @Override public void onCompletion(RecordMetadata metadata, @Nullable Exception exception) {
        try (CurrentTraceContext.Scope ws = current.maybeScope(span.context())) {
          delegate.onCompletion(metadata, exception);
        } finally {
          super.onCompletion(metadata, exception);
        }
      }
    }
  }

  static final class KafkaProducerAdapter<K, V> implements
    MessageProducerAdapter<ProducerRecord<K, V>>,
    ChannelAdapter<ProducerRecord<K, V>> {
    final KafkaTracing kafkaTracing;

    KafkaProducerAdapter(KafkaTracing kafkaTracing) {
      this.kafkaTracing = kafkaTracing;
    }

    static <K, V> KafkaProducerAdapter<K, V> create(KafkaTracing kafkaTracing) {
      return new KafkaProducerAdapter<>(kafkaTracing);
    }

    @Override public String channel(ProducerRecord message) {
      return message.topic();
    }

    @Override public String operation(ProducerRecord message) {
      return KafkaTracing.PRODUCER_OPERATION;
    }

    @Override public String identifier(ProducerRecord message) {
      return kafkaTracing.recordKey(message.key());
    }

    @Override public String remoteServiceName(ProducerRecord message) {
      return kafkaTracing.remoteServiceName;
    }

    @Override public String channelTagKey(ProducerRecord<K, V> message) {
      return kafkaTracing.channelTagKey(message);
    }

    @Override public String identifierTagKey() {
      return kafkaTracing.identifierTagKey();
    }
  }
}
