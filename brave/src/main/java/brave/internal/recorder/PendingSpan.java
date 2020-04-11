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
package brave.internal.recorder;

import brave.Clock;
import brave.handler.MutableSpan;
import brave.internal.Nullable;
import brave.propagation.TraceContext;
import java.lang.ref.WeakReference;

/**
 * This includes a weak reference of the trace context, which allows externalized forms of the trace
 * context to be swapped for the one in use.
 *
 * <p>This is a weak reference to ensure that {@link PendingSpans} can clean up on GC.
 */
public final class PendingSpan extends WeakReference<TraceContext> {
  final MutableSpan state;
  final TickClock clock;
  volatile Throwable caller;

  PendingSpan(TraceContext context, MutableSpan state, TickClock clock) {
    super(context);
    this.state = state;
    this.clock = clock;
  }

  /** Returns the context for this span unless it was cleared due to GC. */
  @Nullable public TraceContext context() {
    return get();
  }

  /** Returns the state currently accumulated for this trace ID and span ID */
  public MutableSpan state() {
    return state;
  }

  /** Returns a clock that ensures startTimestamp consistency across the trace */
  public Clock clock() {
    return clock;
  }
}
