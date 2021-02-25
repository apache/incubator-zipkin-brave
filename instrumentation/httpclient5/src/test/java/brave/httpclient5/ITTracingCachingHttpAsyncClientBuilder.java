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

import brave.handler.MutableSpan;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.SamplingFlags;
import brave.propagation.TraceContext;
import java.io.IOException;
import java.util.Arrays;
import okhttp3.mockwebserver.MockResponse;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.cache.CachingHttpAsyncClientBuilder;
import org.junit.Test;

import static brave.Span.Kind.CLIENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ITTracingCachingHttpAsyncClientBuilder extends ITTracingHttpAsyncClientBuilder {
  @Override
  protected HttpAsyncClientBuilder newClientBuilder() {
    return CachingHttpAsyncClientBuilder.create().disableAutomaticRetries();
  }

  /**
   * Handle when the client doesn't actually make a client span
   */
  @Test
  public void cacheControl() throws IOException {
    server.enqueue(new MockResponse()
      .addHeader("Content-Type", "text/plain")
      .addHeader("Cache-Control", "max-age=600, stale-while-revalidate=1200")
      .setBody("Hello"));

    TraceContext parent = newTraceContext(SamplingFlags.SAMPLED);
    try (Scope scope = currentTraceContext.newScope(parent)) {
      get(client, "/cached");
      get(client, "/cached");
    }

    assertThat(server.getRequestCount()).isEqualTo(1);

    MutableSpan real = testSpanHandler.takeRemoteSpan(CLIENT);
    MutableSpan cached = testSpanHandler.takeLocalSpan();
    assertThat(cached.tags()).containsKey("http.cache_hit");

    for (MutableSpan child : Arrays.asList(real, cached)) {
      assertChildOf(child, parent);
    }
    assertSequential(real, cached);
  }

  /**
   * Caching Client will throw error asynchronously.
   */
  @Override
  @Test
  public void failedRequestInterceptorRemovesScope() {
    assertThat(currentTraceContext.get()).isNull();
    RuntimeException error = new RuntimeException("Test");
    client = HttpClient5Tracing.newBuilder(httpTracing)
      .build(newClientBuilder()
        .addRequestInterceptorLast((httpRequest, entityDetails, httpContext) -> {
          throw error;
        }));
    client.start();

    assertThatThrownBy(() -> get(client, "/foo"))
      .hasRootCause(error);

    assertThat(currentTraceContext.get()).isNull();

    testSpanHandler.takeRemoteSpanWithError(CLIENT, error);
  }
}
