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

import brave.Span;
import brave.http.HttpClientHandler;
import brave.http.HttpClientRequest;
import brave.http.HttpClientResponse;
import java.io.IOException;
import org.apache.hc.client5.http.async.AsyncExecCallback;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.nio.AsyncDataConsumer;

class AsyncExecCallbackWrapper implements AsyncExecCallback {

  final AsyncExecCallback asyncExecCallback;
  final HttpClientHandler<HttpClientRequest, HttpClientResponse> handler;
  final Span span;
  final HttpRequestWrapper requestWrapper;
  final HttpClientContext context;

  AsyncExecCallbackWrapper(
    final AsyncExecCallback asyncExecCallback, HttpRequestWrapper requestWrapper,
    HttpClientHandler<HttpClientRequest, HttpClientResponse> handler,
    Span span, HttpClientContext context) {
    this.asyncExecCallback = asyncExecCallback;
    this.requestWrapper = requestWrapper;
    this.handler = handler;
    this.span = span;
    this.context = context;
  }

  @Override
  public AsyncDataConsumer handleResponse(
    final HttpResponse response,
    final EntityDetails entityDetails) throws HttpException, IOException {
    handleSpan(response);
    return asyncExecCallback.handleResponse(response, entityDetails);
  }

  @Override
  public void handleInformationResponse(final HttpResponse response)
    throws HttpException, IOException {
    handleSpan(response);
    asyncExecCallback.handleInformationResponse(response);
  }

  @Override
  public void completed() {
    asyncExecCallback.completed();
  }

  @Override
  public void failed(final Exception cause) {
    handler.handleReceive(new HttpResponseWrapper(null, requestWrapper, cause), span);
    asyncExecCallback.failed(cause);
    // Handle scope if exception is raised after receiving.
    HttpClientUtils.closeScope(context);
  }

  private void handleSpan(HttpResponse response) {
    context.removeAttribute(Span.class.getName());
    if (HttpClientUtils.isLocalCached(context, span)) {
      span.kind(null);
    }
    handler.handleReceive(new HttpResponseWrapper(response, requestWrapper, null), span);
  }
}
