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
package brave.messaging;

import brave.Span;
import brave.Tracer;
import brave.propagation.TraceContext;

public class MessagingProducerHandler<P, Chan, Msg>
  extends MessagingHandler<Chan, Msg, ChannelAdapter<Chan>, MessageProducerAdapter<Msg>> {

  public static <P, Chan, Msg> MessagingProducerHandler<P, Chan, Msg> create(
    P delegate,
    MessagingTracing tracing,
    ChannelAdapter<Chan> channelAdapter,
    MessageProducerAdapter<Msg> messageAdapter,
    TraceContext.Extractor<Msg> extractor,
    TraceContext.Injector<Msg> injector) {
    return new MessagingProducerHandler<>(delegate, tracing, channelAdapter, messageAdapter,
      extractor, injector);
  }

  public final P delegate;
  final Tracer tracer;

  public MessagingProducerHandler(
    P delegate,
    MessagingTracing messagingTracing,
    ChannelAdapter<Chan> channelAdapter,
    MessageProducerAdapter<Msg> messageAdapter,
    TraceContext.Extractor<Msg> extractor,
    TraceContext.Injector<Msg> injector) {
    super(messagingTracing.tracing.currentTraceContext(), channelAdapter, messageAdapter,
      messagingTracing.producerParser, extractor, injector);
    this.delegate = delegate;
    this.tracer = messagingTracing.tracing.tracer();
  }

  public Span handleProduce(Chan channel, Msg message) {
    TraceContext maybeParent = currentTraceContext.get();
    // Unlike message consumers, we try current span before trying extraction. This is the proper
    // order because the span in scope should take precedence over a potentially stale header entry.
    //
    // NOTE: Brave instrumentation used properly does not result in stale header entries, as we
    // always clear message headers after reading.
    Span span;
    if (maybeParent == null) {
      span = tracer.nextSpan(extractor.extract(message));
    } else {
      // As JMS is sensitive about write access to headers, we  defensively clear even if it seems
      // upstream would have cleared (because there is a span in scope!).
      span = tracer.newChild(maybeParent);
      //TODO check we dont need this
      // messageAdapter.clearPropagation(message);
    }

    if (!span.isNoop()) {
      span.kind(Span.Kind.PRODUCER).name(messageAdapter.operation(message));
      parser.message(channelAdapter, messageAdapter, channel, message, span);
      String remoteServiceName = channelAdapter.remoteServiceName(channel);
      if (remoteServiceName != null) span.remoteServiceName(remoteServiceName);
      span.start();
    }

    injector.inject(span.context(), message);

    return span;
  }
}
