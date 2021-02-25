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
import brave.http.HttpTracing;
import brave.propagation.TraceContext;
import java.io.IOException;
import org.apache.hc.client5.http.async.AsyncExecCallback;
import org.apache.hc.client5.http.async.AsyncExecChain;
import org.apache.hc.client5.http.async.AsyncExecChainHandler;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.nio.AsyncEntityProducer;

import static brave.httpclient5.HttpClientUtils.parseTargetAddress;

class AsyncHandleSendHandler implements AsyncExecChainHandler {
  final HttpClientHandler<HttpClientRequest, HttpClientResponse> handler;

  AsyncHandleSendHandler(HttpTracing httpTracing) {
    this.handler = HttpClientHandler.create(httpTracing);
  }

  @Override
  public void execute(HttpRequest request, AsyncEntityProducer entityProducer,
    AsyncExecChain.Scope scope,
    AsyncExecChain chain, AsyncExecCallback asyncExecCallback)
    throws HttpException, IOException {
    HttpClientContext context = scope.clientContext;
    TraceContext parent = (TraceContext) context.getAttribute(TraceContext.class.getName());

    HttpRequestWrapper requestWrapper =
      new HttpRequestWrapper(request, scope.route.getTargetHost());
    Span span = handler.handleSendWithParent(requestWrapper, parent);
    context.setAttribute(Span.class.getName(), span);

    parseTargetAddress(requestWrapper.target, span);
    AsyncExecCallbackWrapper callbackWrapper =
      new AsyncExecCallbackWrapper(asyncExecCallback, requestWrapper, handler, span, context);
    try {
      chain.proceed(request, entityProducer, scope, callbackWrapper);
    } catch (Throwable e) {
      // Handle if exception is raised before sending.
      context.removeAttribute(Span.class.getName());
      HttpClientUtils.closeScope(context);
      handler.handleReceive(new HttpResponseWrapper(null, requestWrapper, e), span);
      throw e;
    }
  }
}
