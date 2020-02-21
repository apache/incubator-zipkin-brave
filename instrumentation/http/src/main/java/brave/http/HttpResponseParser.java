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
package brave.http;

import brave.ErrorParser;
import brave.SpanCustomizer;
import brave.internal.Nullable;
import brave.propagation.ExtraFieldPropagation;
import brave.propagation.TraceContext;

/**
 * Use this to control the response data recorded for an {@link TraceContext#sampledLocal() sampled
 * HTTP client or server span}.
 *
 * <p>Here's an example that adds all HTTP status codes, not just the error ones.
 * <pre>{@code
 * httpTracing = httpTracing.toBuilder()
 *   .clientResponseParser((response, error, context, span) -> {
 *     HttpResponseParser.DEFAULT.parse(response, error, context, span);
 *     int statusCode = response != null ? response.statusCode() : 0;
 *     if (statusCode > 0) span.tag("http.status_code", String.valueOf(statusCode));
 *   }).build();
 * }</pre>
 *
 * @see HttpRequestParser
 */
// @FunctionalInterface: do not add methods as it will break api
public interface HttpResponseParser {
  HttpResponseParser DEFAULT = new HttpResponseParser.Default();

  /**
   * Implement to choose what data from the http response are parsed into the span representing it.
   *
   * <p>Note: Either the response or error parameters may be null, but not both.
   *
   * <p>Note: This is called after {@link ErrorParser#error(Throwable, SpanCustomizer)}, which means
   * any "error" tag set will likely overwrite what the error parser set.
   *
   * @see Default
   */
  void parse(@Nullable HttpResponse response, @Nullable Throwable error, TraceContext context,
    SpanCustomizer customizer);

  /**
   * The default data policy sets the span name to the HTTP route when available, and sets the and
   * adds the "http.status_code" and "error" tags.
   *
   * <p><h3>Route-based span name</h3>
   * If routing is supported, and a GET didn't match due to 404, the span name will be "get
   * not_found". If it didn't match due to redirect, the span name will be "get redirected". If
   * routing is not supported, the span name is left alone.
   */
  // This accepts response or exception because sometimes http 500 is an exception and sometimes not
  // If this were not an abstraction, we'd use separate hooks for response and error.
  class Default implements HttpResponseParser {

    /**
     * This tags "http.status_code" when it is not 2xx. If the there is no exception and the status
     * code is neither 2xx nor 3xx, it tags "error". This also overrides the span name based on the
     * {@link HttpResponse#method()} and {@link HttpResponse#route()} where possible (ex "get
     * /users/:userId").
     *
     * <p>If you only want to change how exceptions are parsed, override {@link #error(int,
     * Throwable, TraceContext, SpanCustomizer)} instead.
     */
    @Override public void parse(@Nullable HttpResponse response, @Nullable Throwable error,
      TraceContext context, SpanCustomizer customizer) {
      int statusCode = 0;
      if (response != null) {
        statusCode = response.statusCode();
        String nameFromRoute = spanNameFromRoute(response, statusCode);
        if (nameFromRoute != null) customizer.name(nameFromRoute);
        String maybeStatus = maybeStatusAsString(statusCode, 299);
        if (maybeStatus != null) customizer.tag("http.status_code", maybeStatus);
      }
      error(statusCode, error, context, customizer);
    }

    /** The intent of this is to by default add "http.status_code", when not a success code */
    @Nullable String maybeStatusAsString(int statusCode, int upperRange) {
      if (statusCode != 0 && (statusCode < 200 || statusCode > upperRange)) {
        return String.valueOf(statusCode);
      }
      return null;
    }

    static String spanNameFromRoute(HttpResponse response, int statusCode) {
      String method = response.method();
      if (method == null) return null; // don't undo a valid name elsewhere
      String route = response.route();
      if (route == null) return null; // don't undo a valid name elsewhere
      if (!"".equals(route)) return method + " " + route;
      if (statusCode / 100 == 3) return method + " redirected";
      if (statusCode == 404) return method + " not_found";
      return null; // unexpected
    }

    /**
     * Override to change what data from the HTTP error are parsed into the span modeling it. By
     * default, this tags "error" as the the status code, if the error parameter was null and the
     * HTTP status is below 1xx or above 3xx.
     *
     * <p>Note: Either the httpStatus can be zero or the error parameter null, but not both. This
     * does not parse the error, as it is assumed that the {@link ErrorParser has done so prior}.
     *
     * <p>Conventionally associated with the tag key "error"
     */
    protected void error(int httpStatus, @Nullable Throwable error, TraceContext context,
      SpanCustomizer customizer) {
      if (error != null) return; // the call site used the ErrorParser

      // Instrumentation error should not make span errors. We don't know the difference between a
      // library being unable to get the http status and a bad status (0). We don't classify zero as
      // error in case instrumentation cannot read the status. This prevents tagging every response as
      // error.
      if (httpStatus == 0) return;

      // Unlike success path tagging, we only want to indicate something as error if it is not in a
      // success range. 1xx-3xx are not errors. It is endpoint-specific if client codes like 404 are
      // in fact errors. That's why this is overridable.

      // 1xx, 2xx, and 3xx codes are not all valid, but the math is good enough vs drift and opinion
      // about individual codes in the range.
      if (httpStatus < 100 || httpStatus > 399) {
        customizer.tag("error", String.valueOf(httpStatus));
      }
    }
  }
}
