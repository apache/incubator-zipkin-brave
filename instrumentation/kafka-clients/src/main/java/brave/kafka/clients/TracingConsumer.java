/*
 * Copyright 2013-2020 The OpenZipkin Authors
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
import brave.messaging.MessagingRequest;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.SamplerFunction;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

/**
 * Kafka Consumer decorator. Read records headers to create and complete a child of the incoming
 * producers span if possible.
 */
final class TracingConsumer<K, V> implements Consumer<K, V> {

  final Consumer<K, V> delegate;
  final KafkaTracing kafkaTracing;
  final Tracing tracing;
  final Extractor<KafkaConsumerRequest> extractor;
  final SamplerFunction<MessagingRequest> sampler;
  final Injector<KafkaConsumerRequest> injector;
  final String remoteServiceName;
  final boolean singleRootSpanOnReceiveBatch;
  final boolean newTraceOnReceive;
  // replicate org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener behaviour
  static final ConsumerRebalanceListener NO_OP_CONSUMER_REBALANCE_LISTENER =
    new ConsumerRebalanceListener() {
      @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      }

      @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      }
    };

  TracingConsumer(Consumer<K, V> delegate, KafkaTracing kafkaTracing) {
    this.delegate = delegate;
    this.kafkaTracing = kafkaTracing;
    this.tracing = kafkaTracing.messagingTracing.tracing();
    this.extractor = kafkaTracing.consumerExtractor;
    this.sampler = kafkaTracing.consumerSampler;
    this.injector = kafkaTracing.consumerInjector;
    this.remoteServiceName = kafkaTracing.remoteServiceName;
    this.singleRootSpanOnReceiveBatch = kafkaTracing.singleRootSpanOnReceiveBatch;
    this.newTraceOnReceive = kafkaTracing.messagingTracing.newTraceOnReceive();
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.0
  public ConsumerRecords<K, V> poll(Duration timeout) {
    return poll(timeout.toMillis());
  }

  /** This uses a single timestamp for all records polled, to reduce overhead. */
  // Do not use @Override annotation to avoid compatibility on deprecated methods
  public ConsumerRecords<K, V> poll(long timeout) {
    ConsumerRecords<K, V> records = delegate.poll(timeout);
    if (records.isEmpty() || tracing.isNoop()) return records;
    long timestamp = 0L;
    Map<String, Span> consumerSpansForTopic = new LinkedHashMap<>();
    for (TopicPartition partition : records.partitions()) {
      String topic = partition.topic();
      List<ConsumerRecord<K, V>> recordsInPartition = records.records(partition);
      for (int i = 0, length = recordsInPartition.size(); i < length; i++) {
        ConsumerRecord<K, V> record = recordsInPartition.get(i);
        KafkaConsumerRequest request = new KafkaConsumerRequest(record);
        TraceContextOrSamplingFlags extracted =
          kafkaTracing.extractAndClearHeaders(extractor, request, record.headers());

        // If we extracted neither a trace context, nor request-scoped data (extra),
        // and sharing trace is enabled make or reuse a span for this topic
        if (extracted.equals(TraceContextOrSamplingFlags.EMPTY) && singleRootSpanOnReceiveBatch) {
          Span span = consumerSpansForTopic.get(topic);
          if (span == null) {
            span = kafkaTracing.nextMessagingSpan(sampler, request, extracted);
            if (!span.isNoop()) {
              setConsumerSpan(topic, span);
              // incur timestamp overhead only once
              if (timestamp == 0L) {
                timestamp = tracing.clock(span.context()).currentTimeMicroseconds();
              }
              span.start(timestamp);
            }
            consumerSpansForTopic.put(topic, span);
          }
          injector.inject(span.context(), request);
        } else { // we extracted request-scoped data, so cannot share a consumer span.
          Span span = newTraceOnReceive ?
              kafkaTracing.newMessagingTrace(sampler, request, extracted) :
              kafkaTracing.nextMessagingSpan(sampler, request, extracted);
          if (!span.isNoop()) {
            setConsumerSpan(topic, span);
            // incur timestamp overhead only once
            if (timestamp == 0L) {
              timestamp = tracing.clock(span.context()).currentTimeMicroseconds();
            }
            span.start(timestamp).finish(timestamp); // span won't be shared by other records
          }
          injector.inject(span.context(), request);
        }
      }
    }
    for (Span span : consumerSpansForTopic.values()) span.finish(timestamp);
    return records;
  }

  @Override public Set<TopicPartition> assignment() {
    return delegate.assignment();
  }

  @Override public Set<String> subscription() {
    return delegate.subscription();
  }

  @Override public void subscribe(Collection<String> topics) {
    delegate.subscribe(topics);
  }

  @Override public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    delegate.subscribe(topics, callback);
  }

  @Override public void assign(Collection<TopicPartition> partitions) {
    delegate.assign(partitions);
  }

  @Override public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    delegate.subscribe(pattern, callback);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 1.0
  public void subscribe(Pattern pattern) {
    delegate.subscribe(pattern, NO_OP_CONSUMER_REBALANCE_LISTENER);
  }

  @Override public void unsubscribe() {
    delegate.unsubscribe();
  }

  @Override public void commitSync() {
    delegate.commitSync();
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.0
  public void commitSync(Duration timeout) {
    delegate.commitSync(timeout);
  }

  @Override public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    delegate.commitSync(offsets);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.0
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
    delegate.commitSync(offsets, timeout);
  }

  @Override public void commitAsync() {
    delegate.commitAsync();
  }

  @Override public void commitAsync(OffsetCommitCallback callback) {
    delegate.commitAsync(callback);
  }

  @Override public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
    OffsetCommitCallback callback) {
    delegate.commitAsync(offsets, callback);
  }

  @Override public void seek(TopicPartition partition, long offset) {
    delegate.seek(partition, offset);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.0
  public void seek(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
    delegate.seek(topicPartition, offsetAndMetadata);
  }

  @Override public void seekToBeginning(Collection<TopicPartition> partitions) {
    delegate.seekToBeginning(partitions);
  }

  @Override public void seekToEnd(Collection<TopicPartition> partitions) {
    delegate.seekToEnd(partitions);
  }

  @Override public long position(TopicPartition partition) {
    return delegate.position(partition);
  }

  public long position(TopicPartition partition, Duration timeout) {
    return delegate.position(partition, timeout);
  }

  @Override public OffsetAndMetadata committed(TopicPartition partition) {
    return delegate.committed(partition);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.0
  public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
    return delegate.committed(partition, timeout);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.4
  public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions) {
    return delegate.committed(partitions);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.4
  public Map<TopicPartition, OffsetAndMetadata> committed(
    Set<TopicPartition> partitions, Duration timeout) {
    return delegate.committed(partitions, timeout);
  }

  @Override public Map<MetricName, ? extends Metric> metrics() {
    return delegate.metrics();
  }

  @Override public List<PartitionInfo> partitionsFor(String topic) {
    return delegate.partitionsFor(topic);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.0
  public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
    return delegate.partitionsFor(topic, timeout);
  }

  @Override public Map<String, List<PartitionInfo>> listTopics() {
    return delegate.listTopics();
  }

  @Override public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
    return delegate.listTopics(timeout);
  }

  @Override public Set<TopicPartition> paused() {
    return delegate.paused();
  }

  @Override public void pause(Collection<TopicPartition> partitions) {
    delegate.pause(partitions);
  }

  @Override public void resume(Collection<TopicPartition> partitions) {
    delegate.resume(partitions);
  }

  @Override public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
    Map<TopicPartition, Long> timestampsToSearch) {
    return delegate.offsetsForTimes(timestampsToSearch);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.0
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
    Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
    return delegate.offsetsForTimes(timestampsToSearch, timeout);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    return delegate.beginningOffsets(partitions);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.0
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions,
    Duration timeout) {
    return delegate.beginningOffsets(partitions, timeout);
  }

  @Override public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    return delegate.endOffsets(partitions);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.0
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions,
    Duration timeout) {
    return delegate.endOffsets(partitions, timeout);
  }

  @Override public void close() {
    delegate.close();
  }

  // Do not use @Override annotation to avoid compatibility on deprecated methods
  public void close(long timeout, TimeUnit unit) {
    delegate.close(timeout, unit);
  }

  // Do not use @Override annotation to avoid compatibility issue version < 2.0
  public void close(Duration timeout) {
    delegate.close(timeout);
  }

  @Override public void wakeup() {
    delegate.wakeup();
  }

  void setConsumerSpan(String topic, Span span) {
    span.name("poll").kind(Span.Kind.CONSUMER).tag(KafkaTags.KAFKA_TOPIC_TAG, topic);
    if (remoteServiceName != null) span.remoteServiceName(remoteServiceName);
  }
}
