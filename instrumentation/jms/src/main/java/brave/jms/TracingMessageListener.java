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
package brave.jms;

import brave.Span;
import brave.Tracer;
import brave.Tracer.SpanInScope;
import brave.Tracing;
import brave.messaging.MessagingRequest;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.SamplerFunction;
import javax.jms.Message;
import javax.jms.MessageListener;

import static brave.Span.Kind.CONSUMER;
import static brave.internal.Throwables.propagateIfFatal;
import static brave.jms.MessageParser.destination;

/**
 * When {@link #addConsumerSpan} this creates 2 spans:
 * <ol>
 *   <li>A duration 1 {@link Span.Kind#CONSUMER} span to represent receipt from the destination</li>
 *   <li>A child span with the duration of the delegated listener</li>
 * </ol>
 *
 * <p>{@link #addConsumerSpan} should only be set when the message consumer is not traced.
 */
final class TracingMessageListener implements MessageListener {
  /** Creates a message listener which also adds a consumer span. */
  static MessageListener create(MessageListener delegate, JmsTracing jmsTracing) {
    if (delegate == null) return null;
    if (delegate instanceof TracingMessageListener) return delegate;
    return new TracingMessageListener(delegate, jmsTracing, true);
  }

  final MessageListener delegate;
  final JmsTracing jmsTracing;
  final Tracing tracing;
  final Tracer tracer;
  final Extractor<MessageConsumerRequest> extractor;
  final Injector<MessageConsumerRequest> injector;
  final SamplerFunction<MessagingRequest> sampler;
  final String remoteServiceName;
  final boolean addConsumerSpan;
  final boolean newTraceOnReceive;

  TracingMessageListener(MessageListener delegate, JmsTracing jmsTracing, boolean addConsumerSpan) {
    this.delegate = delegate;
    this.jmsTracing = jmsTracing;
    this.tracing = jmsTracing.tracing;
    this.tracer = jmsTracing.tracer;
    this.extractor = jmsTracing.messageConsumerExtractor;
    this.sampler = jmsTracing.consumerSampler;
    this.injector = jmsTracing.messageConsumerInjector;
    this.remoteServiceName = jmsTracing.remoteServiceName;
    this.addConsumerSpan = addConsumerSpan;
    this.newTraceOnReceive = jmsTracing.messagingTracing.newTraceOnReceive();
  }

  @Override public void onMessage(Message message) {
    Span listenerSpan = startMessageListenerSpan(message);
    SpanInScope ws = tracer.withSpanInScope(listenerSpan);
    Throwable error = null;
    try {
      delegate.onMessage(message);
    } catch (Throwable t) {
      propagateIfFatal(t);
      error = t;
      throw t;
    } finally {
      if (error != null) listenerSpan.error(error);
      listenerSpan.finish();
      ws.close();
    }
  }

  Span startMessageListenerSpan(Message message) {
    if (!addConsumerSpan) return jmsTracing.nextSpan(message).name("on-message").start();

    MessageConsumerRequest request = new MessageConsumerRequest(message, destination(message));

    TraceContextOrSamplingFlags extracted =
      jmsTracing.extractAndClearProperties(extractor, request, message);
    Span consumerSpan = newTraceOnReceive ?
        jmsTracing.newMessagingTrace(sampler, request, extracted) :
        jmsTracing.nextMessagingSpan(sampler, request, extracted);
    // JMS has no visibility of the incoming message, which incidentally could be local!
    consumerSpan.kind(CONSUMER).name("receive");
    Span listenerSpan = tracer.newChild(consumerSpan.context());

    if (!consumerSpan.isNoop()) {
      long timestamp = tracing.clock(consumerSpan.context()).currentTimeMicroseconds();
      consumerSpan.start(timestamp);
      if (remoteServiceName != null) consumerSpan.remoteServiceName(remoteServiceName);
      jmsTracing.tagQueueOrTopic(request, consumerSpan);
      long consumerFinish = timestamp + 1L; // save a clock reading
      consumerSpan.finish(consumerFinish);

      // not using scoped span as we want to start late
      listenerSpan.name("on-message").start(consumerFinish);
    }
    return listenerSpan;
  }
}
