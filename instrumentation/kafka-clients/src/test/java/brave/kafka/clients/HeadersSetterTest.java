/*
 * Copyright 2013-2019 The OpenZipkin Authors
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
package brave.kafka.clients;

import brave.propagation.Propagation;
import brave.test.propagation.PropagationSetterTest;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import static brave.kafka.clients.KafkaPropagation.HEADERS_SETTER;
import static java.nio.charset.StandardCharsets.UTF_8;

public class HeadersSetterTest extends PropagationSetterTest<Headers, String> {
  Headers carrier = new RecordHeaders();

  @Override public Propagation.KeyFactory<String> keyFactory() {
    return Propagation.KeyFactory.STRING;
  }

  @Override protected Headers carrier() {
    return carrier;
  }

  @Override protected Propagation.Setter<Headers, String> setter() {
    return HEADERS_SETTER;
  }

  @Override protected Iterable<String> read(Headers carrier, String key) {
    return StreamSupport.stream(carrier.headers(key).spliterator(), false)
      .map(h -> new String(h.value(), UTF_8))
      .collect(Collectors.toList());
  }
}
