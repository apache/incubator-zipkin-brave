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
package brave.handler;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.internal.Nullable;
import brave.propagation.TraceContext;
import java.lang.ref.WeakReference;

/**
 * This tracks one recording of a {@link TraceContext}. Common implementations include span
 * reporting (ex to Zipkin) and data manipulation, such as redaction for security purposes.
 *
 * <h3>Relationship to Span lifecycle</h3>
 * The pair of {@link #begin} and {@link #end} seems the same as the span lifecycle. In most cases
 * it will be the same, but you cannot assume this.
 *
 * <p>A {@link TraceContext} could be recorded twice, for example, if a long operation
 * began, called {@link Span#flush()} (recording 1) and later called {@link Span#finish()}
 * (recording 2). A {@link TraceContext} could be disrupted by garbage collection resulting in a
 * {@link Cause#ABANDONED}. A user could even {@linkplain Cause#ABANDONED abandon} a span without
 * recording anything!
 *
 * <p>Collectors that process finished spans will need to look at the {@link Cause} and {@link
 * MutableSpan} collected. For example, {@link Cause#FINISHED} is usually a good enough heuristic to
 * find complete spans.
 *
 * <h3>Advanced Notes</h3>
 * <p>It is important to do work quickly as callbacks are run on the same thread as application
 * code. However, do not mutate {@link MutableSpan} between callbacks, as it is not thread safe.
 *
 * <p>The {@link TraceContext} and {@link MutableSpan} parameter from {@link #begin} will be
 * the same reference for {@link #end}.
 *
 * <p>If caching the context or span parameters between callbacks, consider a {@link WeakReference}
 * to avoid holding up garbage collection.
 *
 * <p>The {@link #begin} callback primarily supports tracking of children, or partitioning of
 * data for backend that needs to see an entire {@linkplain TraceContext#localRootId() local root}.
 *
 * @since 5.12
 */
public abstract class SpanHandler {
  /**
   * What ended the data collection?
   *
   * @since 5.12
   */
  public enum Cause {
    /**
     * Called on {@link Span#abandon()}.
     *
     * <p>This is useful when counting children. Decrement your counter when this occurs as the
     * span will not be reported.
     *
     * <p><em>Note:</em>Abandoned spans should be ignored as they aren't indicative of an error.
     * Some instrumentation speculatively create a span for possible outcomes such as retry.
     */
    ABANDONED,
    /**
     * Called on {@link Span#finish()} and is the simplest cause to reason with. When {@link
     * MutableSpan#startTimestamp()} is present, you can assume with high confidence you have all
     * recorded data for this span.
     */
    FINISHED,
    /**
     * Called on {@link Span#flush()}.
     *
     * <p>Even though the span here is incomplete (missing {@link MutableSpan#finishTimestamp()},
     * it is reported to the tracing system unless a {@link SpanHandler} returns false.
     */
    FLUSHED,
    /**
     * Called when the trace context was garbage collected prior to completion.
     *
     * <p>Normally, {@link #end(TraceContext, MutableSpan, Cause)} is only called upon explicit
     * termination of a span: {@link Span#finish()}, {@link Span#finish(long)} or {@link
     * Span#flush()}. Upon this cause, the callback will also receive data orphaned due to spans
     * being never terminated or data added after termination.
     *
     * <p><em>Note</em>: If you are doing redaction, you should redact for all causes, not just
     * {@link #FINISHED}, as orphans may have sensitive data also.
     *
     * <h3>What is an orphaned span?</h3>
     *
     * <p>An orphan is  when data remains associated with a span when it is garbage collected. This
     * is almost always a bug. For example, calling {@link Span#tag(String, String)} after calling
     * {@link Span#finish()}, or calling {@link Tracer#nextSpan()} yet never using the result. To
     * track down bugs like this, set the logger {@link brave.Tracer} to FINE level.
     *
     * <h3>Why handle orphaned spans?</h3>
     *
     * <p>Use cases for handling orphans logging a different way than default, or incrementing bug
     * counters. For example, you could use the same credit card cleaner here as you do on the
     * success path.
     *
     * <h3>What shouldn't handle orphaned spans?</h3>
     *
     * <p>As this is related to bugs, no assumptions can be made about span count etc. For example,
     * one span context can result in many calls to this handler, unrelated to the actual operation
     * performed. Handlers that redact or clean data work for normal spans and orphans. However,
     * aggregation handlers, such as dependency linkers or success/fail counters, can create
     * problems if used against orphaned spans.
     *
     * <h2>Implementation</h2>
     * <p>The {@link MutableSpan} parameter to {@link #end(TraceContext, MutableSpan, Cause)}
     * includes data configured by default and any state that was was orphaned (ex a tag). You
     * cannot assume the span has a {@link MutableSpan#startTimestamp()} for example.
     */
    ORPHANED
  }

  /**
   * Use to avoid comparing against {@code null} references.
   *
   * @since 5.12
   */
  public static final SpanHandler NOOP = new SpanHandler() {
    @Override public String toString() {
      return "NoopSpanHandler{}";
    }
  };

  /**
   * This is called when a span is sampled, but before it is started.
   *
   * @param context the trace context which is  {@link TraceContext#sampledLocal()}. This includes
   * identifiers and potentially {@link TraceContext#extra() extra propagated data} such as baggage
   * or extended sampling configuration.
   * @param span a mutable object that stores data recorded with span apis. Modifications are
   * visible to later collectors.
   * @param parent can be {@code null} only when the new context is a {@linkplain
   * TraceContext#isLocalRoot() local root}.
   * @return {@code true} retains the span, and should almost always be used. {@code false} makes it
   * invisible to later handlers such as Zipkin.
   * @see Tracing.Builder#alwaysSampleLocal()
   * @see #end(TraceContext, MutableSpan, Cause)
   * @since 5.12
   */
  public boolean begin(TraceContext context, MutableSpan span, @Nullable TraceContext parent) {
    return true;
  }

  /**
   * Called when data collection complete.
   *
   * <h3>Advanced Note</h3>
   * By default, this only receives callbacks when data is intended to be recorded. If you are
   * implementing tracking between {@link #begin} and here, you should consider overriding {@link
   * #handlesAbandoned()} so that you have parity for all cases.
   *
   * @param context same instance as passed to {@link #begin}
   * @param span same instance as passed to {@link #begin}
   * @param cause why the data collection stopped.
   * @return {@code true} retains the span, and should almost always be used. {@code false} drops
   * the span, making it invisible to later handlers such as Zipkin.
   * @see #begin(TraceContext, MutableSpan, TraceContext)
   * @see Cause
   * @since 5.12
   */
  public boolean end(TraceContext context, MutableSpan span, Cause cause) {
    return true;
  }

  /**
   * {@link Span#abandon()} means the data is not intended to be recorded. It results in an
   * {@linkplain #end(TraceContext, MutableSpan, Cause) end callback} with {@link Cause#ABANDONED}.
   *
   *
   * <p><em>Note</em>: {@link Cause#ABANDONED} means the data is not intended to be recorded!
   */
  public boolean handlesAbandoned() {
    return false;
  }
}
