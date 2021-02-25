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
import org.apache.hc.core5.http.EntityDetails;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpRequestInterceptor;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpResponseInterceptor;
import org.apache.hc.core5.http.protocol.HttpContext;

class TraceContextOpenScopeInterceptor implements HttpRequestInterceptor,
  HttpResponseInterceptor {

  final CurrentTraceContext currentTraceContext;

  public TraceContextOpenScopeInterceptor(CurrentTraceContext currentTraceContext) {
    this.currentTraceContext = currentTraceContext;
  }

  @Override public void process(HttpRequest httpRequest, EntityDetails entityDetails,
    HttpContext httpContext) {
    HttpClientUtils.openScope(httpContext, currentTraceContext);
  }

  @Override public void process(HttpResponse httpResponse,
    EntityDetails entityDetails, HttpContext httpContext) {
    HttpClientUtils.openScope(httpContext, currentTraceContext);
  }
}
