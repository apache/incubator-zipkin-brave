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
package brave.spring.rabbit;

import brave.Span;
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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.aopalliance.aop.Advice;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

/**
 * Factory for Brave instrumented Spring Rabbit classes.
 */
public final class SpringRabbitTracing {

  static final String
    RABBIT_EXCHANGE = "rabbit.exchange",
    RABBIT_ROUTING_KEY = "rabbit.routing_key",
    RABBIT_QUEUE = "rabbit.queue";

  public static SpringRabbitTracing create(Tracing tracing) {
    return newBuilder(tracing).build();
  }

  /** @since 5.9 */
  public static SpringRabbitTracing create(MessagingTracing messagingTracing) {
    return newBuilder(messagingTracing).build();
  }

  public static Builder newBuilder(Tracing tracing) {
    return newBuilder(MessagingTracing.create(tracing));
  }

  /** @since 5.9 */
  public static Builder newBuilder(MessagingTracing messagingTracing) {
    return new Builder(messagingTracing);
  }

  public static final class Builder {
    final MessagingTracing messagingTracing;
    String remoteServiceName = "rabbitmq";

    Builder(MessagingTracing messagingTracing) {
      if (messagingTracing == null) throw new NullPointerException("messagingTracing == null");
      this.messagingTracing = messagingTracing;
    }

    /**
     * The remote service name that describes the broker in the dependency graph. Defaults to
     * "rabbitmq"
     */
    public Builder remoteServiceName(String remoteServiceName) {
      this.remoteServiceName = remoteServiceName;
      return this;
    }

    /**
     * @deprecated as of v5.9, this is ignored because single format is default for messaging. Use
     * {@link B3Propagation#newFactoryBuilder()} to change the default.
     */
    @Deprecated public Builder writeB3SingleFormat(boolean writeB3SingleFormat) {
      return this;
    }

    public SpringRabbitTracing build() {
      return new SpringRabbitTracing(this);
    }
  }

  final Tracing tracing;
  final Tracer tracer;
  final MessagingTracing messagingTracing;
  final Extractor<MessageProducerRequest> producerExtractor;
  final Extractor<MessageConsumerRequest> consumerExtractor;
  final Extractor<MessageProperties> processorExtractor;
  final Injector<MessageProducerRequest> producerInjector;
  final Injector<MessageConsumerRequest> consumerInjector;
  final SamplerFunction<MessagingRequest> producerSampler, consumerSampler;
  final String[] propagationKeys;
  final String remoteServiceName;
  final Field beforePublishPostProcessorsField;

  SpringRabbitTracing(Builder builder) { // intentionally hidden constructor
    this.tracing = builder.messagingTracing.tracing();
    this.tracer = tracing.tracer();
    this.messagingTracing = builder.messagingTracing;
    MessagingTracing messagingTracing = builder.messagingTracing;
    Propagation<String> propagation = tracing.propagation();
    this.producerExtractor = propagation.extractor(MessageProducerRequest.GETTER);
    this.consumerExtractor = propagation.extractor(MessageConsumerRequest.GETTER);
    this.processorExtractor = propagation.extractor(SpringRabbitPropagation.GETTER);
    this.producerInjector = propagation.injector(MessageProducerRequest.SETTER);
    this.consumerInjector = propagation.injector(MessageConsumerRequest.SETTER);
    this.producerSampler = messagingTracing.producerSampler();
    this.consumerSampler = messagingTracing.consumerSampler();
    this.propagationKeys = propagation.keys().toArray(new String[0]);
    this.remoteServiceName = builder.remoteServiceName;
    Field beforePublishPostProcessorsField = null;
    try {
      beforePublishPostProcessorsField =
        RabbitTemplate.class.getDeclaredField("beforePublishPostProcessors");
      beforePublishPostProcessorsField.setAccessible(true);
    } catch (NoSuchFieldException e) {
    }
    this.beforePublishPostProcessorsField = beforePublishPostProcessorsField;
  }

  /** Creates an instrumented {@linkplain RabbitTemplate} */
  public RabbitTemplate newRabbitTemplate(ConnectionFactory connectionFactory) {
    RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
    TracingMessagePostProcessor tracingMessagePostProcessor = new TracingMessagePostProcessor(this);
    rabbitTemplate.setBeforePublishPostProcessors(tracingMessagePostProcessor);
    return rabbitTemplate;
  }

  /** Instruments an existing {@linkplain RabbitTemplate} */
  public RabbitTemplate decorateRabbitTemplate(RabbitTemplate rabbitTemplate) {
    // Skip out if we can't read the field for the existing post processors
    if (beforePublishPostProcessorsField == null) return rabbitTemplate;
    Collection<MessagePostProcessor> processors;
    try {
      processors = (Collection) beforePublishPostProcessorsField.get(rabbitTemplate);
    } catch (IllegalAccessException e) {
      return rabbitTemplate;
    }

    TracingMessagePostProcessor tracingMessagePostProcessor = new TracingMessagePostProcessor(this);
    // If there are no existing post processors, return only the tracing one
    if (processors == null) {
      rabbitTemplate.setBeforePublishPostProcessors(tracingMessagePostProcessor);
      return rabbitTemplate;
    }

    // If there is an existing tracing post processor return
    for (MessagePostProcessor processor : processors) {
      if (processor instanceof TracingMessagePostProcessor) {
        return rabbitTemplate;
      }
    }

    // Otherwise, add ours and return
    List<MessagePostProcessor> newProcessors = new ArrayList<>(processors.size() + 1);
    newProcessors.addAll(processors);
    newProcessors.add(tracingMessagePostProcessor);
    rabbitTemplate.setBeforePublishPostProcessors(
      newProcessors.toArray(new MessagePostProcessor[0])
    );
    return rabbitTemplate;
  }

  /** Creates an instrumented {@linkplain SimpleRabbitListenerContainerFactory} */
  public SimpleRabbitListenerContainerFactory newSimpleRabbitListenerContainerFactory(
    ConnectionFactory connectionFactory
  ) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    factory.setConnectionFactory(connectionFactory);
    factory.setAdviceChain(new TracingRabbitListenerAdvice(this));
    return factory;
  }

  /** Instruments an existing {@linkplain SimpleRabbitListenerContainerFactory} */
  public SimpleRabbitListenerContainerFactory decorateSimpleRabbitListenerContainerFactory(
    SimpleRabbitListenerContainerFactory factory
  ) {
    Advice[] chain = factory.getAdviceChain();

    TracingRabbitListenerAdvice tracingAdvice = new TracingRabbitListenerAdvice(this);
    // If there are no existing advice, return only the tracing one
    if (chain == null) {
      factory.setAdviceChain(tracingAdvice);
      return factory;
    }

    // If there is an existing tracing advice return
    for (Advice advice : chain) {
      if (advice instanceof TracingRabbitListenerAdvice) {
        return factory;
      }
    }

    // Otherwise, add ours and return
    Advice[] newChain = new Advice[chain.length + 1];
    newChain[0] = tracingAdvice;
    System.arraycopy(chain, 0, newChain, 1, chain.length);
    factory.setAdviceChain(newChain);
    return factory;
  }

  <R> TraceContextOrSamplingFlags extractAndClearHeaders(
    Extractor<R> extractor, R request, Message message
  ) {
    TraceContextOrSamplingFlags extracted = extractor.extract(request);
    // Clear any propagation keys present in the headers
    if (!extracted.equals(TraceContextOrSamplingFlags.EMPTY)) {
      MessageProperties properties = message.getMessageProperties();
      if (properties != null) clearHeaders(properties.getHeaders());
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

  void clearHeaders(Map<String, Object> headers) {
    for (String key : propagationKeys) headers.remove(key);
  }
}
