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

import brave.http.HttpClientRequest;
import brave.internal.Nullable;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;

final class HttpRequestWrapper extends HttpClientRequest {
  final HttpRequest request;
  @Nullable final HttpHost target;

  HttpRequestWrapper(HttpRequest request, @Nullable HttpHost target) {
    this.request = request;
    this.target = target;
  }

  @Override
  public Object unwrap() {
    return request;
  }

  @Override
  public String method() {
    return request.getMethod();
  }

  @Override
  public String path() {
    String result = request.getPath();
    int queryIndex = result.indexOf('?');
    return queryIndex == -1 ? result : result.substring(0, queryIndex);
  }

  @Override
  public String url() {
    if (target != null) {
      return target.toURI() + request.getRequestUri();
    }
    return request.getRequestUri();
  }

  @Override
  @Nullable
  public String header(String name) {
    Header result = request.getFirstHeader(name);
    return result != null ? result.getValue() : null;
  }

  @Override
  public void header(String name, String value) {
    request.setHeader(name, value);
  }
}
