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

import brave.Tracing;
import brave.http.HttpTracing;
import brave.propagation.CurrentTraceContext;
import org.apache.hc.client5.http.impl.ChainElement;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.H2AsyncClientBuilder;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;

import static org.apache.hc.client5.http.impl.ChainElement.PROTOCOL;

public class HttpClient5Tracing {

  final HttpTracing httpTracing;

  final CurrentTraceContext currentTraceContext;

  final TraceContextOpenScopeInterceptor openInterceptor;

  final TraceContextCloseScopeInterceptor closeInterceptor;

  HttpClient5Tracing(HttpTracing httpTracing) {
    if (httpTracing == null) throw new NullPointerException("HttpTracing == null");
    this.httpTracing = httpTracing;
    this.openInterceptor =
      new TraceContextOpenScopeInterceptor(httpTracing.tracing().currentTraceContext());
    this.closeInterceptor = new TraceContextCloseScopeInterceptor();
    this.currentTraceContext = httpTracing.tracing().currentTraceContext();
  }

  public static HttpClient5Tracing newBuilder(Tracing tracing) {
    return new HttpClient5Tracing(HttpTracing.create(tracing));
  }

  public static HttpClient5Tracing newBuilder(HttpTracing httpTracing) {
    return new HttpClient5Tracing(httpTracing);
  }

  public CloseableHttpClient build(HttpClientBuilder builder) {
    if (builder == null) throw new NullPointerException("HttpClientBuilder == null");
    builder.addExecInterceptorBefore(ChainElement.MAIN_TRANSPORT.name(),
      HandleSendHandler.class.getName(),
      new HandleSendHandler(httpTracing));
    builder.addExecInterceptorBefore(ChainElement.PROTOCOL.name(),
      HandleReceiveHandler.class.getName(),
      new HandleReceiveHandler(httpTracing));
    return builder.build();
  }

  public CloseableHttpAsyncClient build(HttpAsyncClientBuilder builder) {
    if (builder == null) throw new NullPointerException("HttpAsyncClientBuilder == null");
    builder.addExecInterceptorBefore(PROTOCOL.name(), AsyncHandleSendHandler.class.getName(),
      new AsyncHandleSendHandler(httpTracing));
    builder.addRequestInterceptorFirst(openInterceptor);
    builder.addRequestInterceptorLast(closeInterceptor);
    builder.addResponseInterceptorFirst(openInterceptor);
    builder.addResponseInterceptorLast(closeInterceptor);
    return new TracingHttpAsyncClient(builder.build(), currentTraceContext);
  }

  public CloseableHttpAsyncClient build(H2AsyncClientBuilder builder) {
    if (builder == null) throw new NullPointerException("H2AsyncClientBuilder == null");
    builder.addExecInterceptorBefore(PROTOCOL.name(), AsyncHandleSendHandler.class.getName(),
      new AsyncHandleSendHandler(httpTracing));
    builder.addRequestInterceptorFirst(openInterceptor);
    builder.addRequestInterceptorLast(closeInterceptor);
    builder.addResponseInterceptorFirst(openInterceptor);
    builder.addResponseInterceptorLast(closeInterceptor);
    return new TracingHttpAsyncClient(builder.build(), currentTraceContext);
  }
}
