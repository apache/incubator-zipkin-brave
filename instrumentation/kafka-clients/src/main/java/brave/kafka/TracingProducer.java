package brave.kafka;

import brave.Span;
import brave.Span.Kind;
import brave.Tracing;
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
import org.apache.kafka.common.errors.ProducerFencedException;

import static brave.kafka.KafkaTags.KAFKA_KEY_TAG;
import static brave.kafka.KafkaTags.KAFKA_TOPIC_TAG;

/**
 *
 */
class TracingProducer<K, V> implements Producer<K, V> {

  private final Tracing tracing;
  private final Producer<K, V> wrappedProducer;

  TracingProducer(Tracing tracing, Producer<K, V> producer) {
    this.wrappedProducer = producer;
    this.tracing = tracing;
  }

  @Override
  public void initTransactions() {
    wrappedProducer.initTransactions();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    wrappedProducer.beginTransaction();
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    wrappedProducer.commitTransaction();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    wrappedProducer.abortTransaction();
  }

  /**
   * Send with a callback is always called for KafkaProducer. We do the same here to enable
   * tracing.
   */
  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return this.send(record, null);
  }

  /**
   * We wrap the send method to add tracing.
   */
  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    Span span = tracing.tracer().nextSpan();
    addZipkinTags(record, span);

    tracing.propagation()
        .injector(new KafkaPropagation.ProducerInjector())
        .inject(span.context(), record);

    Future<RecordMetadata> recordMetadataFuture = wrappedProducer.send(record, callback);
    span.kind(Kind.PRODUCER).start().finish();

    return recordMetadataFuture;
  }

  @Override
  public void flush() {
    wrappedProducer.flush();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return wrappedProducer.partitionsFor(topic);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return wrappedProducer.metrics();
  }

  @Override
  public void close() {
    wrappedProducer.close();
  }

  @Override
  public void close(long timeout, TimeUnit unit) {
    wrappedProducer.close(timeout, unit);
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
      String consumerGroupId)
      throws ProducerFencedException {
    wrappedProducer.sendOffsetsToTransaction(offsets, consumerGroupId);
  }

  private void addZipkinTags(ProducerRecord<K, V> record, Span span) {
    if (record.key() != null) {
      span.tag(KAFKA_KEY_TAG, record.key().toString());
    }
    span.tag(KAFKA_TOPIC_TAG, record.topic());
  }
}
