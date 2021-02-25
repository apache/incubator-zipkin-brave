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
import brave.propagation.CurrentTraceContext;
import java.net.InetAddress;
import org.apache.hc.client5.http.cache.CacheResponseStatus;
import org.apache.hc.client5.http.cache.HttpCacheContext;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.protocol.HttpContext;

class HttpClientUtils {

  static void openScope(HttpContext httpContext, CurrentTraceContext currentTraceContext) {
    Span span = (Span) httpContext.getAttribute(Span.class.getName());
    httpContext.setAttribute(CurrentTraceContext.Scope.class.getName(),
      currentTraceContext.newScope(span.context()));
  }

  static void closeScope(HttpContext httpContext) {
    CurrentTraceContext.Scope scope =
      (CurrentTraceContext.Scope) httpContext.removeAttribute(
        CurrentTraceContext.Scope.class.getName());
    if (scope == null) {
      return;
    }
    scope.close();
  }

  static void parseTargetAddress(HttpHost target, Span span) {
    if (span.isNoop()) {
      return;
    }
    InetAddress address = target.getAddress();
    if (address != null) {
      if (span.remoteIpAndPort(address.getHostAddress(), target.getPort())) {
        return;
      }
    }
    span.remoteIpAndPort(target.getHostName(), target.getPort());
  }

  static boolean isLocalCached(HttpContext context, Span span) {
    boolean cacheHit = CacheResponseStatus.CACHE_HIT == context.getAttribute(
      HttpCacheContext.CACHE_RESPONSE_STATUS);
    if (cacheHit) {
      span.tag("http.cache_hit", "");
    }
    return cacheHit;
  }
}
