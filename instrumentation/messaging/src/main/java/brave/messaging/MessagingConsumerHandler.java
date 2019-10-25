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
import brave.SpanCustomizer;
import brave.Tracing;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import java.util.List;
import java.util.Map;

public class MessagingConsumerHandler<C, Chan, Msg>
  extends MessagingHandler<Chan, Msg, ChannelAdapter<Chan>, MessageAdapter<Msg>> {

  static public <C, Chan, Msg> MessagingConsumerHandler<C, Chan, Msg> create(
    C delegate,
    MessagingTracing tracing,
    ChannelAdapter<Chan> channelAdapter,
    MessageConsumerAdapter<Msg> messageAdapter,
    TraceContext.Extractor<Msg> extractor,
    TraceContext.Injector<Msg> injector) {
    return new MessagingConsumerHandler<>(delegate, tracing, channelAdapter, messageAdapter,
      extractor, injector);
  }

  public final C delegate;
  final Tracing tracing;

  public MessagingConsumerHandler(
    C delegate,
    MessagingTracing messagingTracing,
    ChannelAdapter<Chan> channelAdapter,
    MessageConsumerAdapter<Msg> messageAdapter,
    TraceContext.Extractor<Msg> extractor,
    TraceContext.Injector<Msg> injector) {
    super(messagingTracing.tracing.currentTraceContext(), channelAdapter, messageAdapter,
      messagingTracing.consumerParser, extractor, injector);
    this.delegate = delegate;
    this.tracing = messagingTracing.tracing;
  }

  public Span nextSpan(Chan channel, Msg message) {
    TraceContextOrSamplingFlags extracted = extractor.extract(message);
    Span result = tracing.tracer().nextSpan(extracted);
    if (extracted.context() == null && !result.isNoop()) {
      addTags(channel, result);
    }
    return result;
  }

  /** When an upstream context was not present, lookup keys are unlikely added */
  void addTags(Chan channel, SpanCustomizer result) {
    parser.channel(channelAdapter, channel, result);
    //parser.identifier(messageAdapter, message, result);
  }

  public void handleConsume(Chan channel, Msg message) {
    if (message == null || tracing.isNoop()) return;
    // remove prior propagation headers from the message
    Span span = nextSpan(channel, message);
    if (!span.isNoop()) {
      span.kind(Span.Kind.CONSUMER);
      parser.message(channelAdapter, messageAdapter, channel, message, span);

      // incur timestamp overhead only once
      long timestamp = tracing.clock(span.context()).currentTimeMicroseconds();
      span.start(timestamp).finish(timestamp);
    }
    injector.inject(span.context(), message);
  }

  public Map<String, Span> handleConsume(Chan chan, List<Msg> messages,
    Map<String, Span> spanForChannel) {
    long timestamp = 0L;
    for (int i = 0, length = messages.size(); i < length; i++) {
      Msg message = messages.get(i);
      TraceContextOrSamplingFlags extracted = extractor.extract(message);

      // If we extracted neither a trace context, nor request-scoped data (extra),
      // make or reuse a span for this topic
      if (extracted.samplingFlags() != null && extracted.extra().isEmpty()) {
        String channel = channelAdapter.channel(chan);
        Span span = spanForChannel.get(channel);
        if (span == null) {
          span = tracing.tracer().nextSpan(extracted);
          if (!span.isNoop()) {
            span.name(messageAdapter.operation(message)).kind(Span.Kind.CONSUMER);
            parser.message(channelAdapter, messageAdapter, chan, message, span);
            String remoteServiceName = channelAdapter.remoteServiceName(chan);
            if (remoteServiceName != null) span.remoteServiceName(remoteServiceName);
            // incur timestamp overhead only once
            if (timestamp == 0L) {
              timestamp = tracing.clock(span.context()).currentTimeMicroseconds();
            }
            span.start(timestamp);
          }
          spanForChannel.put(channel, span);
        }
        injector.inject(span.context(), message);
      } else { // we extracted request-scoped data, so cannot share a consumer span.
        Span span = tracing.tracer().nextSpan(extracted);
        if (!span.isNoop()) {
          span.kind(Span.Kind.CONSUMER);
          parser.message(channelAdapter, messageAdapter, chan, message, span);
          String remoteServiceName = channelAdapter.remoteServiceName(chan);
          if (remoteServiceName != null) span.remoteServiceName(remoteServiceName);
          // incur timestamp overhead only once
          if (timestamp == 0L) {
            timestamp = tracing.clock(span.context()).currentTimeMicroseconds();
          }
          span.start(timestamp).finish(timestamp); // span won't be shared by other records
        }
        injector.inject(span.context(), message);
      }
    }
    return spanForChannel;
  }
}
