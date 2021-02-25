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

import brave.http.HttpClientResponse;
import brave.internal.Nullable;
import org.apache.hc.core5.http.HttpResponse;

final class HttpResponseWrapper extends HttpClientResponse {
  @Nullable final HttpRequestWrapper request;
  @Nullable final HttpResponse response;
  @Nullable final Throwable error;

  HttpResponseWrapper(@Nullable HttpResponse response, @Nullable HttpRequestWrapper request,
    @Nullable Throwable error) {
    this.request = request;
    this.response = response;
    this.error = error;
  }

  @Override
  @Nullable
  public Object unwrap() {
    return response;
  }

  @Override
  @Nullable
  public HttpRequestWrapper request() {
    return request;
  }

  @Override
  public Throwable error() {
    return error;
  }

  @Override
  public int statusCode() {
    if (response == null) {
      return 0;
    }
    return response.getCode();
  }
}
