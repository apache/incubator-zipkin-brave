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
import brave.SpanCustomizer;
import brave.Tracer;
import brave.Tracing;
import brave.messaging.MessagingRequest;
import brave.messaging.MessagingTracing;
import brave.propagation.B3Propagation;
import brave.propagation.Propagation;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.SamplerFunction;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

/** Use this class to decorate your Kafka consumer / producer and enable Tracing. */
public final class KafkaTracing {
  public static KafkaTracing create(Tracing tracing) {
    return newBuilder(tracing).build();
  }

  /** @since 5.9 */
  public static KafkaTracing create(MessagingTracing messagingTracing) {
    return newBuilder(messagingTracing).build();
  }

  public static Builder newBuilder(Tracing tracing) {
    return newBuilder(MessagingTracing.create(tracing));
  }

  /** @since 5.9 */
  public static Builder newBuilder(MessagingTracing messagingTracing) {
    return new Builder(messagingTracing);
  }

  /** @since 5.10 **/
  public Builder toBuilder() {
    return new Builder(this);
  }

  public static final class Builder {
    final MessagingTracing messagingTracing;
    String remoteServiceName = "kafka";
    boolean singleRootSpanOnReceiveBatch = true;

    Builder(MessagingTracing messagingTracing) {
      if (messagingTracing == null) throw new NullPointerException("messagingTracing == null");
      this.messagingTracing = messagingTracing;
    }

    Builder(KafkaTracing kafkaTracing) {
      this.messagingTracing = kafkaTracing.messagingTracing;
      this.remoteServiceName = kafkaTracing.remoteServiceName;
      this.singleRootSpanOnReceiveBatch = kafkaTracing.singleRootSpanOnReceiveBatch;
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
     * Controls the sharing of a {@code poll} span for incoming spans with no trace context.
     *
     * <p>If true, all the spans received in a poll batch that do not have trace-context will be
     * added to a single new {@code poll} root span. Otherwise, a {@code poll} span will be created
     * for each such message.
     *
     * @since 5.10
     */
    public Builder singleRootSpanOnReceiveBatch(boolean singleRootSpanOnReceiveBatch) {
      this.singleRootSpanOnReceiveBatch = singleRootSpanOnReceiveBatch;
      return this;
    }

    /**
     * @deprecated as of v5.9, this is ignored because single format is default for messaging. Use
     * {@link B3Propagation#newFactoryBuilder()} to change the default.
     */
    @Deprecated public Builder writeB3SingleFormat(boolean writeB3SingleFormat) {
      return this;
    }

    public KafkaTracing build() {
      return new KafkaTracing(this);
    }
  }

  final MessagingTracing messagingTracing;
  final Tracer tracer;
  final Extractor<KafkaProducerRequest> producerExtractor;
  final Extractor<KafkaConsumerRequest> consumerExtractor;
  final Extractor<Headers> processorExtractor;
  final Injector<KafkaProducerRequest> producerInjector;
  final Injector<KafkaConsumerRequest> consumerInjector;
  final SamplerFunction<MessagingRequest> producerSampler, consumerSampler;
  final Set<String> propagationKeys;
  final String remoteServiceName;
  final boolean singleRootSpanOnReceiveBatch;

  KafkaTracing(Builder builder) { // intentionally hidden constructor
    this.messagingTracing = builder.messagingTracing;
    this.tracer = builder.messagingTracing.tracing().tracer();
    Propagation<String> propagation = messagingTracing.tracing().propagation();
    this.producerExtractor = propagation.extractor(KafkaProducerRequest::getHeader);
    this.consumerExtractor = propagation.extractor(KafkaConsumerRequest::getHeader);
    this.processorExtractor = propagation.extractor(KafkaPropagation.GETTER);
    this.producerInjector = propagation.injector(KafkaProducerRequest::setHeader);
    this.consumerInjector = propagation.injector(KafkaConsumerRequest::setHeader);
    this.producerSampler = messagingTracing.producerSampler();
    this.consumerSampler = messagingTracing.consumerSampler();
    this.propagationKeys = new LinkedHashSet<>(propagation.keys());
    this.remoteServiceName = builder.remoteServiceName;
    this.singleRootSpanOnReceiveBatch = builder.singleRootSpanOnReceiveBatch;
  }

  /** @since 5.9 exposed for Kafka Streams tracing. */
  public MessagingTracing messagingTracing() {
    return messagingTracing;
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
  public Span nextSpan(ConsumerRecord<?, ?> record) {
    // Eventhough the type is ConsumerRecord, this is not a (remote) consumer span. Only "poll"
    // events create consumer spans. Since this is a processor span, we use the normal sampler.
    TraceContextOrSamplingFlags extracted =
      extractAndClearHeaders(processorExtractor, record.headers(), record.headers());
    Span result = tracer.nextSpan(extracted);
    if (extracted.context() == null && !result.isNoop()) {
      addTags(record, result);
    }
    return result;
  }

  <R> TraceContextOrSamplingFlags extractAndClearHeaders(
    Extractor<R> extractor, R request, Headers headers
  ) {
    TraceContextOrSamplingFlags extracted = extractor.extract(request);
    // Clear any propagation keys present in the headers
    if (!extracted.equals(TraceContextOrSamplingFlags.EMPTY)) {
      clearHeaders(headers);
    }
    return extracted;
  }

  /** Creates a potentially noop remote span representing this request */
  Span nextMessagingSpan(
    SamplerFunction<MessagingRequest> sampler,
    MessagingRequest request,
    TraceContextOrSamplingFlags extracted
  ) {
    Boolean sampled = extracted.sampled();
    // only recreate the context if the messaging sampler made a decision
    if (sampled == null && (sampled = sampler.trySample(request)) != null) {
      extracted = extracted.sampled(sampled.booleanValue());
    }
    return tracer.nextSpan(extracted);
  }

  Span newMessagingTrace(
    SamplerFunction<MessagingRequest> sampler,
    MessagingRequest request,
    TraceContextOrSamplingFlags extracted
  ) {
    String traceId = null, spanId = null;
    if (extracted.context() != null) {
      traceId = extracted.context().traceIdString();
      if (extracted.context().spanIdString() != null) {
        spanId = extracted.context().spanIdString();
      }
    }
    Boolean sampled = extracted.sampled();
    if (sampled == null && (sampled = sampler.trySample(request)) != null) {
      extracted = extracted.sampled(sampled.booleanValue());
    }
    Span span;
    if (extracted.samplingFlags() != null) {
      extracted = TraceContextOrSamplingFlags.create(extracted.samplingFlags());
      span = tracer.nextSpan(extracted);
    } else {
      span = tracer.newTrace();
    }
    if (traceId != null) span.tag("parent.trace_id", traceId);
    if (spanId != null) span.tag("parent.span_id", spanId);
    return span;
  }

  // We can't just skip clearing headers we use because we might inject B3 single, yet have stale B3
  // multi, or visa versa.
  void clearHeaders(Headers headers) {
    // Headers::remove creates and consumes an iterator each time. This does one loop instead.
    for (Iterator<Header> i = headers.iterator(); i.hasNext(); ) {
      Header next = i.next();
      if (propagationKeys.contains(next.key())) i.remove();
    }
  }

  /** When an upstream context was not present, lookup keys are unlikely added */
  static void addTags(ConsumerRecord<?, ?> record, SpanCustomizer result) {
    if (record.key() instanceof String && !"".equals(record.key())) {
      result.tag(KafkaTags.KAFKA_KEY_TAG, record.key().toString());
    }
    result.tag(KafkaTags.KAFKA_TOPIC_TAG, record.topic());
  }
}
