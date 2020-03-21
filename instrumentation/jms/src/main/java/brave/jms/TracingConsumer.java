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
import brave.Tracing;
import brave.internal.Nullable;
import brave.messaging.MessagingRequest;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.SamplerFunction;
import javax.jms.Destination;
import javax.jms.Message;

abstract class TracingConsumer<C> {
  final C delegate;
  final JmsTracing jmsTracing;
  final Tracing tracing;
  final Extractor<MessageConsumerRequest> extractor;
  final Injector<MessageConsumerRequest> injector;
  final SamplerFunction<MessagingRequest> sampler;
  @Nullable final String remoteServiceName;
  final boolean newTraceOnReceive;

  TracingConsumer(C delegate, JmsTracing jmsTracing) {
    this.delegate = delegate;
    this.jmsTracing = jmsTracing;
    this.tracing = jmsTracing.tracing;
    this.extractor = jmsTracing.messageConsumerExtractor;
    this.sampler = jmsTracing.consumerSampler;
    this.injector = jmsTracing.messageConsumerInjector;
    this.remoteServiceName = jmsTracing.remoteServiceName;
    this.newTraceOnReceive = jmsTracing.messagingTracing.newTraceOnReceive();
  }

  void handleReceive(Message message) {
    if (message == null || tracing.isNoop()) return;
    MessageConsumerRequest request = new MessageConsumerRequest(message, destination(message));

    TraceContextOrSamplingFlags extracted =
        jmsTracing.extractAndClearProperties(extractor, request, message);
    Span span = newTraceOnReceive ?
        jmsTracing.newMessagingTrace(sampler, request, extracted) :
        jmsTracing.nextMessagingSpan(sampler, request, extracted);

    if (!span.isNoop()) {
      span.name("receive").kind(Span.Kind.CONSUMER);
      Destination destination = destination(message);
      if (destination != null) jmsTracing.tagQueueOrTopic(request, span);
      if (remoteServiceName != null) span.remoteServiceName(remoteServiceName);

      // incur timestamp overhead only once
      long timestamp = tracing.clock(span.context()).currentTimeMicroseconds();
      span.start(timestamp).finish(timestamp);
    }
    injector.inject(span.context(), request);
  }

  abstract @Nullable Destination destination(Message message);
}
