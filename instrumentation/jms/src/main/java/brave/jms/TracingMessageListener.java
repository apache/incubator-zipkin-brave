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
package brave.jms;

import brave.Span;
import brave.Tracer;
import brave.Tracer.SpanInScope;
import brave.Tracing;
import brave.messaging.ChannelAdapter;
import brave.propagation.TraceContextOrSamplingFlags;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;

import static brave.Span.Kind.CONSUMER;

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
    if (delegate instanceof TracingMessageListener) return delegate;
    return new TracingMessageListener(delegate, jmsTracing, true);
  }

  final MessageListener delegate;
  final JmsTracing jmsTracing;
  final Tracing tracing;
  final Tracer tracer;
  final String remoteServiceName;
  final boolean addConsumerSpan;
  final ChannelAdapter<Destination> channelAdapter;

  TracingMessageListener(MessageListener delegate, JmsTracing jmsTracing, boolean addConsumerSpan) {
    this.delegate = delegate;
    this.jmsTracing = jmsTracing;
    this.tracing = jmsTracing.msgTracing.tracing();
    this.tracer = jmsTracing.msgTracing.tracing().tracer();
    this.remoteServiceName = jmsTracing.remoteServiceName;
    this.addConsumerSpan = addConsumerSpan;
    channelAdapter = JmsAdapter.JmsChannelAdapter.create(jmsTracing);
  }

  @Override public void onMessage(Message message) {
    Span listenerSpan = startMessageListenerSpan(message);
    try (SpanInScope ws = tracer.withSpanInScope(listenerSpan)) {
      delegate.onMessage(message);
    } catch (Throwable t) {
      listenerSpan.error(t);
      throw t;
    } finally {
      listenerSpan.finish();
    }
  }

  Span startMessageListenerSpan(Message message) {
    if (!addConsumerSpan) return jmsTracing.nextSpan(message).name("on-message").start();
    TraceContextOrSamplingFlags extracted = jmsTracing.extractor.extract(message);

    // JMS has no visibility of the incoming message, which incidentally could be local!
    Span consumerSpan = tracer.nextSpan(extracted).kind(CONSUMER).name("receive");
    Span listenerSpan = tracer.newChild(consumerSpan.context());

    if (!consumerSpan.isNoop()) {
      long timestamp = tracing.clock(consumerSpan.context()).currentTimeMicroseconds();
      consumerSpan.start(timestamp);
      if (remoteServiceName != null) consumerSpan.remoteServiceName(remoteServiceName);
      jmsTracing.msgTracing.consumerParser()
        .channel(channelAdapter, jmsTracing.destination(message), consumerSpan);
      long consumerFinish = timestamp + 1L; // save a clock reading
      consumerSpan.finish(consumerFinish);

      // not using scoped span as we want to start late
      listenerSpan.name("on-message").start(consumerFinish);
    }
    return listenerSpan;
  }
}
