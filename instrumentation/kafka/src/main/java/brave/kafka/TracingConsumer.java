package brave.kafka;

import brave.Tracing;
import java.util.Collection;
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

public class TracingConsumer<K, V> implements Consumer<K, V> {

  private final RecordTracing recordTracing;
  private Consumer<K, V> wrappedConsumer;

  public TracingConsumer(Tracing tracing, Consumer<K, V> consumer) {
    this.recordTracing = new RecordTracing(tracing);
    this.wrappedConsumer = consumer;
  }

  @Override
  public ConsumerRecords<K, V> poll(long timeout) {
    ConsumerRecords<K, V> records = wrappedConsumer.poll(timeout);
    for (ConsumerRecord<K, V> record : records) {
      recordTracing.finishProducerSpan(record);
    }
    return records;
  }

  @Override
  public Set<TopicPartition> assignment() {
    return wrappedConsumer.assignment();
  }

  @Override
  public Set<String> subscription() {
    return wrappedConsumer.subscription();
  }

  @Override
  public void subscribe(Collection<String> topics) {
    wrappedConsumer.subscribe(topics);
  }

  @Override
  public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
    wrappedConsumer.subscribe(topics, callback);
  }

  @Override
  public void assign(Collection<TopicPartition> partitions) {
    wrappedConsumer.assign(partitions);
  }

  @Override
  public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
    wrappedConsumer.subscribe(pattern, callback);
  }

  @Override
  public void unsubscribe() {
    wrappedConsumer.unsubscribe();
  }

  @Override
  public void commitSync() {
    wrappedConsumer.commitSync();
  }

  @Override
  public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
    wrappedConsumer.commitSync(offsets);
  }

  @Override
  public void commitAsync() {
    wrappedConsumer.commitAsync();
  }

  @Override
  public void commitAsync(OffsetCommitCallback callback) {
    wrappedConsumer.commitAsync(callback);
  }

  @Override
  public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
      OffsetCommitCallback callback) {
    wrappedConsumer.commitAsync(offsets, callback);
  }

  @Override
  public void seek(TopicPartition partition, long offset) {
    wrappedConsumer.seek(partition, offset);
  }

  @Override
  public void seekToBeginning(Collection<TopicPartition> partitions) {
    wrappedConsumer.seekToBeginning(partitions);
  }

  @Override
  public void seekToEnd(Collection<TopicPartition> partitions) {
    wrappedConsumer.seekToEnd(partitions);
  }

  @Override
  public long position(TopicPartition partition) {
    return wrappedConsumer.position(partition);
  }

  @Override
  public OffsetAndMetadata committed(TopicPartition partition) {
    return wrappedConsumer.committed(partition);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return wrappedConsumer.metrics();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return wrappedConsumer.partitionsFor(topic);
  }

  @Override
  public Map<String, List<PartitionInfo>> listTopics() {
    return wrappedConsumer.listTopics();
  }

  @Override
  public Set<TopicPartition> paused() {
    return wrappedConsumer.paused();
  }

  @Override
  public void pause(Collection<TopicPartition> partitions) {
    wrappedConsumer.pause(partitions);
  }

  @Override
  public void resume(Collection<TopicPartition> partitions) {
    wrappedConsumer.resume(partitions);
  }

  @Override
  public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
      Map<TopicPartition, Long> timestampsToSearch) {
    return wrappedConsumer.offsetsForTimes(timestampsToSearch);
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
    return wrappedConsumer.beginningOffsets(partitions);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
    return wrappedConsumer.endOffsets(partitions);
  }

  @Override
  public void close() {
    wrappedConsumer.close();
  }

  @Override
  public void close(long timeout, TimeUnit unit) {
    wrappedConsumer.close(timeout, unit);
  }

  @Override
  public void wakeup() {
    wrappedConsumer.wakeup();
  }
}
