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

import brave.SpanCustomizer;

public class MessagingParser {

  public <Chan, Msg> void message(ChannelAdapter<Chan> channelAdapter,
    MessageAdapter<Msg> messageAdapter,
    Chan channel, Msg message, SpanCustomizer customizer) {
    customizer.name(messageAdapter.operation(message));
    channel(channelAdapter, channel, customizer);
    identifier(messageAdapter, message, customizer);
  }

  public <Chan> void channel(ChannelAdapter<Chan> adapter, Chan chan,
    SpanCustomizer customizer) {
    String channel = adapter.channel(chan);
    if (chan != null) customizer.tag(adapter.channelTagKey(chan), channel);
  }

  public <Msg> void identifier(MessageAdapter<Msg> adapter, Msg message,
    SpanCustomizer customizer) {
    String identifier = adapter.identifier(message);
    if (identifier != null) {
      customizer.tag(adapter.identifierTagKey(), identifier);
    }
  }

  //public <Msg> TraceContextOrSamplingFlags extractContextAndClearMessage(
  //    MessageAdapter<Msg> adapter,
  //    TraceContext.Extractor<Msg> extractor,
  //    Msg message) {
  //  TraceContextOrSamplingFlags extracted = extractor.extract(message);
  //  // clear propagation headers if we were able to extract a span
  //  //TODO check if correct to not filter on empty flags. Diff between kafka and jms instrumentation
  //  //if (!extracted.equals(TraceContextOrSamplingFlags.EMPTY)) {
  //  //TODO check we dont need this
  //  //  adapter.clearPropagation(message);
  //  //}
  //  return extracted;
  //}
}
