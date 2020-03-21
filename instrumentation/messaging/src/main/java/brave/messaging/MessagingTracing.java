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

import brave.Tracing;
import brave.internal.Nullable;
import brave.sampler.SamplerFunction;
import brave.sampler.SamplerFunctions;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Instances built via {@link #create(Tracing)} or {@link #newBuilder(Tracing)} are registered
 * automatically such that statically configured instrumentation like HTTP producers can use {@link
 * #current()}.
 *
 * @since 5.9
 */
public class MessagingTracing implements Closeable {
  // AtomicReference<Object> instead of AtomicReference<MessagingTracing> to ensure unloadable
  static final AtomicReference<Object> CURRENT = new AtomicReference<>();

  /** @since 5.9 */
  public static MessagingTracing create(Tracing tracing) {
    return newBuilder(tracing).build();
  }

  /** @since 5.9 */
  public static Builder newBuilder(Tracing tracing) {
    return new Builder(tracing);
  }

  /** @since 5.9 */
  public Tracing tracing() {
    return tracing;
  }

  /**
   * Returns an overriding sampling decision for a new trace. Defaults to ignore the request and use
   * the {@link SamplerFunctions#deferDecision() trace ID instead}.
   *
   * <p>This decision happens when a trace was not yet started in process. For example, you may be
   * making an messaging request as a part of booting your application. You may want to opt-out of
   * tracing producer requests that did not originate from a consumer request.
   *
   * @see SamplerFunctions
   * @see MessagingRuleSampler
   * @since 5.9
   */
  public SamplerFunction<MessagingRequest> producerSampler() {
    return producerSampler;
  }

  /**
   * Returns an overriding sampling decision for a new trace. Defaults to ignore the request and use
   * the {@link SamplerFunctions#deferDecision() trace ID instead}.
   *
   * <p>This decision happens when trace IDs were not in headers, or a sampling decision has not
   * yet been made. For example, if a trace is already in progress, this function is not called. You
   * can implement this to skip channels that you never want to trace.
   *
   * @see SamplerFunctions
   * @see MessagingRuleSampler
   * @since 5.9
   */
  public SamplerFunction<MessagingRequest> consumerSampler() {
    return consumerSampler;
  }

  /**
   * Returns flag to create a new trace when consuming a message with an existing trace context, or
   * to continue.
   *
   * @since 5.10 (potentially)
   */
  public boolean newTraceOnReceive() {
    return newTraceOnReceive;
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  final Tracing tracing;
  final SamplerFunction<MessagingRequest> producerSampler;
  final SamplerFunction<MessagingRequest> consumerSampler;

  final boolean newTraceOnReceive;

  MessagingTracing(Builder builder) {
    this.tracing = builder.tracing;
    this.producerSampler = builder.producerSampler;
    this.consumerSampler = builder.consumerSampler;
    this.newTraceOnReceive = builder.newTraceOnReceive;
    // assign current IFF there's no instance already current
    CURRENT.compareAndSet(null, this);
  }

  public static final class Builder {
    Tracing tracing;
    SamplerFunction<MessagingRequest> producerSampler;
    SamplerFunction<MessagingRequest> consumerSampler;
    boolean newTraceOnReceive = false;

    Builder(Tracing tracing) {
      if (tracing == null) throw new NullPointerException("tracing == null");
      this.tracing = tracing;
      this.producerSampler = SamplerFunctions.deferDecision();
      this.consumerSampler = SamplerFunctions.deferDecision();
    }

    Builder(MessagingTracing source) {
      this.tracing = source.tracing;
      this.producerSampler = source.producerSampler;
      this.consumerSampler = source.consumerSampler;
      this.newTraceOnReceive = source.newTraceOnReceive;
    }

    /** @see MessagingTracing#tracing() */
    public Builder tracing(Tracing tracing) {
      if (tracing == null) throw new NullPointerException("tracing == null");
      this.tracing = tracing;
      return this;
    }

    /** @see MessagingTracing#producerSampler() */
    public Builder producerSampler(SamplerFunction<MessagingRequest> producerSampler) {
      if (producerSampler == null) throw new NullPointerException("producerSampler == null");
      this.producerSampler = producerSampler;
      return this;
    }

    /**
     * @see MessagingTracing#consumerSampler()
     */
    public Builder consumerSampler(SamplerFunction<MessagingRequest> consumerSampler) {
      if (consumerSampler == null) throw new NullPointerException("consumerSampler == null");
      this.consumerSampler = consumerSampler;
      return this;
    }

    /**
     * @see MessagingTracing#newTraceOnReceive()
     */
    public Builder newTraceOnReceive(boolean newTraceOnReceive) {
      this.newTraceOnReceive = newTraceOnReceive;
      return this;
    }

    public MessagingTracing build() {
      return new MessagingTracing(this);
    }
  }

  /**
   * Returns the most recently created tracing component iff it hasn't been closed. null otherwise.
   *
   * <p>This object should not be cached.
   *
   * @since 5.9
   */
  @Nullable public static MessagingTracing current() {
    return (MessagingTracing) CURRENT.get();
  }

  /** @since 5.9 */
  @Override public void close() {
    // only set null if we are the outer-most instance
    CURRENT.compareAndSet(this, null);
  }
}
