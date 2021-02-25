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

import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HttpResponseWrapperTest {
  @Mock HttpRequest request;
  @Mock HttpResponse response;

  @Test public void request() {
    assertThat(
      new HttpResponseWrapper(response, new HttpRequestWrapper(request, null), null).request()
        .unwrap())
      .isSameAs(request);
  }

  @Test public void statusCode() {
    when(response.getCode()).thenReturn(200);
    assertThat(new HttpResponseWrapper(response, new HttpRequestWrapper(request, null),
      null).statusCode()).isEqualTo(200);
  }

  @Test public void statusCode_zeroWhenNoResponse() {
    assertThat(new HttpResponseWrapper(null, new HttpRequestWrapper(request, null),
      null).statusCode()).isZero();
  }
}
