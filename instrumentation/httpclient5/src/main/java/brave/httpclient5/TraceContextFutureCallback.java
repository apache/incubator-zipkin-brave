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

package brave.httpclient5;

import brave.propagation.CurrentTraceContext;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.TraceContext;
import org.apache.hc.core5.concurrent.FutureCallback;

final class TraceContextFutureCallback<T> implements FutureCallback<T> {
  final FutureCallback<T> delegate;
  final CurrentTraceContext currentTraceContext;
  final TraceContext invocationContext;

  TraceContextFutureCallback(FutureCallback<T> delegate,
    CurrentTraceContext currentTraceContext, TraceContext invocationContext) {
    this.delegate = delegate;
    this.currentTraceContext = currentTraceContext;
    this.invocationContext = invocationContext;
  }

  @Override
  public void completed(T t) {
    try (Scope scope = currentTraceContext.maybeScope(invocationContext)) {
      delegate.completed(t);
    }
  }

  @Override
  public void failed(Exception e) {
    try (Scope scope = currentTraceContext.maybeScope(invocationContext)) {
      delegate.failed(e);
    }
  }

  @Override
  public void cancelled() {
    try (Scope scope = currentTraceContext.maybeScope(invocationContext)) {
      delegate.cancelled();
    }
  }

  @Override
  public String toString() {
    return delegate.toString();
  }
}
