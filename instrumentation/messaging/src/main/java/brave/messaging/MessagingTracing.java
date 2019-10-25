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
import brave.Tracing;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;

public class MessagingTracing {

  public static MessagingTracing create(Tracing tracing) {
    return newBuilder(tracing).build();
  }

  public static MessagingTracing.Builder newBuilder(Tracing tracing) {
    return new MessagingTracing.Builder(tracing);
  }

  final Tracing tracing;
  final MessagingConsumerParser consumerParser;
  final MessagingProducerParser producerParser;

  MessagingTracing(Builder builder) {
    this.tracing = builder.tracing;
    this.consumerParser = builder.consumerParser;
    this.producerParser = builder.producerParser;
  }

  public Tracing tracing() {
    return tracing;
  }

  public MessagingProducerParser producerParser() {
    return producerParser;
  }

  public MessagingConsumerParser consumerParser() {
    return consumerParser;
  }

  public <Chan, Msg> Span nextSpan(ChannelAdapter<Chan> channelAdapter,
    MessageAdapter<Msg> messageAdapter,
    TraceContext.Extractor<Msg> extractor,
    Msg message,
    Chan channel) {
    TraceContextOrSamplingFlags extracted = extractor.extract(message);
    Span result = tracing.tracer().nextSpan(extracted);

    // When an upstream context was not present, lookup keys are unlikely added
    if (extracted.context() == null && !result.isNoop()) {
      consumerParser.channel(channelAdapter, channel, result);
      consumerParser.identifier(messageAdapter, message, result);
    }
    return result;
  }

  public static class Builder {
    final Tracing tracing;
    MessagingConsumerParser consumerParser = new MessagingConsumerParser();
    MessagingProducerParser producerParser = new MessagingProducerParser();

    Builder(Tracing tracing) {
      if (tracing == null) throw new NullPointerException("tracing == null");
      this.tracing = tracing;
    }

    public Builder consumerParser(MessagingConsumerParser consumerParser) {
      if (producerParser == null) throw new NullPointerException("consumerParser == null");
      this.consumerParser = consumerParser;
      return this;
    }

    public Builder producerParser(MessagingProducerParser producerParser) {
      if (producerParser == null) throw new NullPointerException("producerParser == null");
      this.producerParser = producerParser;
      return this;
    }

    MessagingTracing build() {
      return new MessagingTracing(this);
    }
  }
}
