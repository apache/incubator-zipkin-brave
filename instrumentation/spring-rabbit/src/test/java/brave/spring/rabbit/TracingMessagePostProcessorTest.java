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
package brave.spring.rabbit;

import brave.Tracing;
import brave.messaging.MessagingTracing;
import brave.propagation.B3SingleFormat;
import brave.propagation.CurrentTraceContext.Scope;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.propagation.TraceContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

public class TracingMessagePostProcessorTest {
  List<Span> spans = new ArrayList<>();
  Tracing tracing = Tracing.newBuilder()
    .currentTraceContext(ThreadLocalCurrentTraceContext.create())
    .spanReporter(spans::add)
    .build();
  TracingMessagePostProcessor tracingMessagePostProcessor = new TracingMessagePostProcessor(
    SpringRabbitTracing.newBuilder(tracing).remoteServiceName("my-exchange").build()
  );

  @After public void close() {
    tracing.close();
  }

  @Test public void should_attempt_to_resume_headers() {
    TraceContext parent = TraceContext.newBuilder().traceId(1L).spanId(1L).sampled(true).build();
    Message message = MessageBuilder.withBody(new byte[0]).build();
    message.getMessageProperties().setHeader("b3", B3SingleFormat.writeB3SingleFormat(parent));

    Message postProcessMessage = tracingMessagePostProcessor.postProcessMessage(message);

    assertThat(spans.get(0).parentId()).isEqualTo(parent.spanIdString());
    Map<String, Object> headers = postProcessMessage.getMessageProperties().getHeaders();
    assertThat(headers.get("b3").toString()).endsWith("-" + spans.get(0).id() + "-1");
  }

  @Test public void should_create_newTrace_whenFlagEnabled() {
    tracingMessagePostProcessor = new TracingMessagePostProcessor(
        SpringRabbitTracing
            .newBuilder(
                MessagingTracing.newBuilder(tracing)
                    .newTraceOnReceive(true)
                    .build())
            .remoteServiceName("my-exchange")
            .build()
    );
    TraceContext parent = TraceContext.newBuilder().traceId(1L).spanId(1L).sampled(true).build();
    Message message = MessageBuilder.withBody(new byte[0]).build();
    message.getMessageProperties().setHeader("b3", B3SingleFormat.writeB3SingleFormat(parent));

    Message postProcessMessage = tracingMessagePostProcessor.postProcessMessage(message);

    Span span = spans.get(0);
    assertThat(span.traceId()).isNotEqualTo(parent.traceIdString());
    assertThat(span.parentId()).isNull();
    assertThat(span.tags().get("parent.trace_id")).isEqualTo(parent.traceIdString());
    assertThat(span.tags().get("parent.span_id")).isEqualTo(parent.spanIdString());
    Map<String, Object> headers = postProcessMessage.getMessageProperties().getHeaders();
    assertThat(headers.get("b3").toString()).endsWith("-" + span.id() + "-1");
  }

  @Test public void should_prefer_current_span() {
    TraceContext grandparent =
      TraceContext.newBuilder().traceId(1L).spanId(1L).sampled(true).build();
    TraceContext parent = grandparent.toBuilder().parentId(grandparent.spanId()).spanId(2L).build();

    // Will be either a bug, or a missing processor stage which can result in an old span in headers
    Message message = MessageBuilder.withBody(new byte[0]).build();
    message.getMessageProperties().setHeader("b3", B3SingleFormat.writeB3SingleFormat(grandparent));

    Message postProcessMessage;
    try (Scope scope = tracing.currentTraceContext().newScope(parent)) {
      postProcessMessage = tracingMessagePostProcessor.postProcessMessage(message);
    }

    assertThat(spans.get(0).parentId()).isEqualTo(parent.spanIdString());
    Map<String, Object> headers = postProcessMessage.getMessageProperties().getHeaders();
    assertThat(headers.get("b3").toString()).endsWith("-" + spans.get(0).id() + "-1");
  }

  @Test public void should_add_b3_headers_to_message() {
    Message message = MessageBuilder.withBody(new byte[0]).build();
    Message postProcessMessage = tracingMessagePostProcessor.postProcessMessage(message);

    Set<String> headerKeys = postProcessMessage.getMessageProperties().getHeaders().keySet();

    assertThat(headerKeys).containsExactly("b3");
  }

  @Test public void should_add_b3_single_header_to_message() {
    TracingMessagePostProcessor tracingMessagePostProcessor = new TracingMessagePostProcessor(
      SpringRabbitTracing.newBuilder(tracing).writeB3SingleFormat(true).build()
    );

    Message message = MessageBuilder.withBody(new byte[0]).build();
    Message postProcessMessage = tracingMessagePostProcessor.postProcessMessage(message);

    assertThat(postProcessMessage.getMessageProperties().getHeaders())
      .containsOnlyKeys("b3");
    assertThat(postProcessMessage.getMessageProperties().getHeaders().get("b3").toString())
      .matches("^[0-9a-f]{16}-[0-9a-f]{16}-1$");
  }

  @Test public void should_report_span() {
    Message message = MessageBuilder.withBody(new byte[0]).build();
    tracingMessagePostProcessor.postProcessMessage(message);

    assertThat(spans).hasSize(1);
  }

  @Test public void should_set_remote_service() {
    Message message = MessageBuilder.withBody(new byte[0]).build();
    tracingMessagePostProcessor.postProcessMessage(message);

    assertThat(spans.get(0).remoteServiceName())
      .isEqualTo("my-exchange");
  }
}
