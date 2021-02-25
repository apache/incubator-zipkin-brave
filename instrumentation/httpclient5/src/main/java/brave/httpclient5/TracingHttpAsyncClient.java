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
import brave.propagation.TraceContext;
import java.io.IOException;
import java.util.concurrent.Future;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.function.Supplier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.AsyncPushConsumer;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
import org.apache.hc.core5.http.nio.HandlerFactory;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.reactor.IOReactorStatus;
import org.apache.hc.core5.util.TimeValue;

class TracingHttpAsyncClient extends CloseableHttpAsyncClient {
  final CloseableHttpAsyncClient delegate;
  final CurrentTraceContext currentTraceContext;

  TracingHttpAsyncClient(CloseableHttpAsyncClient delegate,
    CurrentTraceContext currentTraceContext) {
    this.delegate = delegate;
    this.currentTraceContext = currentTraceContext;
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  public IOReactorStatus getStatus() {
    return delegate.getStatus();
  }

  @Override
  public void awaitShutdown(TimeValue waitTime) throws InterruptedException {
    delegate.awaitShutdown(waitTime);
  }

  @Override
  public void initiateShutdown() {
    delegate.initiateShutdown();
  }

  @Override
  protected <T> Future<T> doExecute(
    HttpHost target, AsyncRequestProducer requestProducer,
    AsyncResponseConsumer<T> responseConsumer,
    HandlerFactory<AsyncPushConsumer> pushHandlerFactory, HttpContext context,
    FutureCallback<T> callback) {
    TraceContext invocationContext = currentTraceContext.get();
    if (invocationContext != null) {
      context.setAttribute(TraceContext.class.getName(), invocationContext);
    }

    if (callback != null && invocationContext != null) {
      callback = new TraceContextFutureCallback<>(callback, currentTraceContext, invocationContext);
    }

    return delegate.execute(target, requestProducer, responseConsumer, pushHandlerFactory, context,
      callback);
  }

  @Override
  public void register(String hostname, String uriPattern, Supplier<AsyncPushConsumer> supplier) {
    delegate.register(hostname, uriPattern, supplier);
  }

  @Override
  public void close(CloseMode closeMode) {
    delegate.close(closeMode);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
