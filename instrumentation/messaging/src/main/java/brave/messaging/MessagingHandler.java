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

import brave.propagation.CurrentTraceContext;
import brave.propagation.TraceContext;

abstract class MessagingHandler<Chan, Msg, CA extends ChannelAdapter<Chan>, MA extends MessageAdapter<Msg>> {

  final CurrentTraceContext currentTraceContext;
  final CA channelAdapter;
  final MA messageAdapter;
  final MessagingParser parser;
  final TraceContext.Extractor<Msg> extractor;
  final TraceContext.Injector<Msg> injector;

  MessagingHandler(
    CurrentTraceContext currentTraceContext,
    CA channelAdapter,
    MA adapter,
    MessagingParser parser,
    TraceContext.Extractor<Msg> extractor,
    TraceContext.Injector<Msg> injector) {
    this.currentTraceContext = currentTraceContext;
    this.channelAdapter = channelAdapter;
    this.messageAdapter = adapter;
    this.parser = parser;
    this.extractor = extractor;
    this.injector = injector;
  }
}
