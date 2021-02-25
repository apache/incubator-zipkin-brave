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
import java.io.IOException;
import org.apache.hc.client5.http.classic.ExecChain;
import org.apache.hc.client5.http.classic.ExecChain.Scope;
import org.apache.hc.client5.http.classic.ExecChainHandler;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpException;

import static brave.httpclient5.HttpClientUtils.parseTargetAddress;

class HandleSendHandler implements ExecChainHandler {

  final HttpClientHandler<HttpClientRequest, HttpClientResponse> handler;

  HandleSendHandler(HttpTracing httpTracing) {
    this.handler = HttpClientHandler.create(httpTracing);
  }

  @Override
  public ClassicHttpResponse execute(
    ClassicHttpRequest classicHttpRequest,
    Scope scope,
    ExecChain execChain) throws IOException, HttpException {

    Span span = (Span) scope.clientContext.removeAttribute(Span.class.getName());

    if (span != null) {
      handler.handleSend(new HttpRequestWrapper(classicHttpRequest, scope.route.getTargetHost()),
        span);
    }

    ClassicHttpResponse response = execChain.proceed(classicHttpRequest, scope);
    if (span != null) {
      parseTargetAddress(scope.route.getTargetHost(), span);
    }
    return response;
  }
}
