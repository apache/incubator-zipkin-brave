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

import brave.Span;
import brave.Tracing;
import brave.propagation.CurrentTraceContext;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import static brave.kafka.clients.KafkaPropagation.HEADERS_GETTER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaTracingTest extends BaseTracingTest {

  @Test public void nextSpan_prefers_b3_header() {
    fakeRecord.headers().add("b3", "0000000000000001-0000000000000002-1".getBytes(UTF_8));

    Span child;
    try (CurrentTraceContext.Scope ws = tracing.currentTraceContext()
      .newScope(TraceContext.newBuilder().traceId(1).spanId(1).build())) {
      child = kafkaTracing.nextSpan(fakeRecord);
    }
    assertThat(child.context().parentId())
      .isEqualTo(2L);
  }

  @Test public void nextSpan_uses_current_context() {
    Span child;
    try (CurrentTraceContext.Scope ws = tracing.currentTraceContext()
      .newScope(TraceContext.newBuilder().traceId(1).spanId(1).build())) {
      child = kafkaTracing.nextSpan(fakeRecord);
    }
    assertThat(child.context().parentId())
      .isEqualTo(1L);
  }

  @Test public void nextSpan_should_create_span_if_no_headers() {
    assertThat(kafkaTracing.nextSpan(fakeRecord)).isNotNull();
  }

  @Test public void nextSpan_should_tag_topic_and_key_when_no_incoming_context() {
    kafkaTracing.nextSpan(fakeRecord).start().finish();

    assertThat(spans)
      .flatExtracting(s -> s.tags().entrySet())
      .containsOnly(entry("kafka.topic", TEST_TOPIC), entry("kafka.key", TEST_KEY));
  }

  @Test public void nextSpan_shouldnt_tag_null_key() {
    fakeRecord = new ConsumerRecord<>(TEST_TOPIC, 0, 1, null, TEST_VALUE);

    kafkaTracing.nextSpan(fakeRecord).start().finish();

    assertThat(spans)
      .flatExtracting(s -> s.tags().entrySet())
      .containsOnly(entry("kafka.topic", TEST_TOPIC));
  }

  @Test public void nextSpan_shouldnt_tag_binary_key() {
    ConsumerRecord<byte[], String> record =
      new ConsumerRecord<>(TEST_TOPIC, 0, 1, new byte[1], TEST_VALUE);

    kafkaTracing.nextSpan(record).start().finish();

    assertThat(spans)
      .flatExtracting(s -> s.tags().entrySet())
      .containsOnly(entry("kafka.topic", TEST_TOPIC));
  }

  /**
   * We assume topic and key are already tagged by the producer span. However, we can change this
   * policy now, or later when dynamic policy is added to KafkaTracing
   */
  @Test public void nextSpan_shouldnt_tag_topic_and_key_when_incoming_context() {
    addB3Headers(fakeRecord);
    kafkaTracing.nextSpan(fakeRecord).start().finish();

    assertThat(spans)
      .flatExtracting(s -> s.tags().entrySet())
      .isEmpty();
  }

  //FIXME remove if we don't have to clear propagation
  //@Test public void nextSpan_should_clear_propagation_headers() {
  //  addB3Headers(fakeRecord);
  //
  //  kafkaTracing.nextSpan(fakeRecord);
  //  assertThat(fakeRecord.headers().toArray()).isEmpty();
  //}

  @Test public void nextSpan_should_not_clear_other_headers() {
    fakeRecord.headers().add("foo", new byte[0]);

    kafkaTracing.nextSpan(fakeRecord);
    assertThat(fakeRecord.headers().headers("foo")).isNotEmpty();
  }

  @Test public void failsFastIfPropagationDoesntSupportSingleHeader() {
    // Fake propagation because B3 by default does support single header extraction!
    Propagation<String> propagation = mock(Propagation.class);
    when(propagation.extractor(HEADERS_GETTER)).thenReturn(carrier -> {
      assertThat(carrier.lastHeader("b3")).isNotNull(); // sanity check
      return TraceContextOrSamplingFlags.EMPTY; // pretend we couldn't parse
    });

    Propagation.Factory propagationFactory = mock(Propagation.Factory.class);
    when(propagationFactory.create(Propagation.KeyFactory.STRING)).thenReturn(propagation);

    assertThatThrownBy(() -> KafkaTracing.newBuilder(
      Tracing.newBuilder().propagationFactory(propagationFactory).build())
      .writeB3SingleFormat(true)
      .build()
    ).hasMessage(
      "KafkaTracing.Builder.writeB3SingleFormat set, but Tracing.Builder.propagationFactory cannot parse this format!");
  }
}
