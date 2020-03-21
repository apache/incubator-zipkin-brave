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
package brave.jms;

import brave.messaging.MessagingRuleSampler;
import brave.messaging.MessagingTracing;
import brave.sampler.Sampler;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import zipkin2.Span;

import static brave.jms.MessagePropagation.GETTER;
import static brave.messaging.MessagingRequestMatchers.operationEquals;
import static org.assertj.core.api.Assertions.assertThat;

/** When adding tests here, also add to {@linkplain brave.jms.ITJms_2_0_TracingMessageConsumer} */
public class ITTracingJMSConsumer extends JmsTest {
  @Rule public TestName testName = new TestName();
  @Rule public ArtemisJmsTestRule jms = new ArtemisJmsTestRule(testName);

  JMSContext tracedContext;
  JMSProducer producer;
  JMSConsumer consumer;
  JMSContext context;

  @Before public void setup() {
    context = jms.newContext();
    producer = context.createProducer();

    setupTracedConsumer(jmsTracing);
  }

  void setupTracedConsumer(JmsTracing jmsTracing) {
    if (consumer != null) consumer.close();
    if (tracedContext != null) tracedContext.close();
    tracedContext = jmsTracing.connectionFactory(jms.factory)
      .createContext(JMSContext.AUTO_ACKNOWLEDGE);
    consumer = tracedContext.createConsumer(jms.queue);
  }

  @After public void tearDownTraced() {
    tracedContext.close();
  }

  @Test public void messageListener_startsNewTrace() throws Exception {
    messageListener_startsNewTrace(() -> producer.send(jms.queue, "foo"));
  }

  @Test public void messageListener_startsNewTrace_bytes() throws Exception {
    messageListener_startsNewTrace(() -> producer.send(jms.queue, new byte[] {1, 2, 3, 4}));
  }

  void messageListener_startsNewTrace(Runnable send) throws Exception {
    consumer.setMessageListener(m -> {
      tracing.tracer().currentSpanCustomizer().name("message-listener");

      // clearing headers ensures later work doesn't try to use the old parent
      String b3 = GETTER.get(m, "b3");
      tracing.tracer().currentSpanCustomizer().tag("b3", String.valueOf(b3 != null));
    });

    send.run();

    Span consumerSpan = takeSpan(), listenerSpan = takeSpan();

    assertThat(consumerSpan.name()).isEqualTo("receive");
    assertThat(consumerSpan.parentId()).isNull(); // root span
    assertThat(consumerSpan.kind()).isEqualTo(Span.Kind.CONSUMER);
    assertThat(consumerSpan.tags())
      .hasSize(1)
      .containsEntry("jms.queue", jms.queueName);

    assertThat(listenerSpan.name()).isEqualTo("message-listener"); // overridden name
    assertThat(listenerSpan.parentId()).isEqualTo(consumerSpan.id()); // root span
    assertThat(listenerSpan.kind()).isNull(); // processor span, not a consumer
    assertThat(listenerSpan.tags())
      .hasSize(1) // no redundant copy of consumer tags
      .containsEntry("b3", "false"); // b3 header not leaked to listener
  }

  @Test public void messageListener_resumesTrace() throws Exception {
    messageListener_resumesTrace(() -> producer.send(jms.queue, "foo"));
  }

  @Test public void messageListener_resumesTrace_bytes() throws Exception {
    messageListener_resumesTrace(() -> producer.send(jms.queue, new byte[] {1, 2, 3, 4}));
  }

  void messageListener_resumesTrace(Runnable send) throws Exception {
    consumer.setMessageListener(m -> {
      // clearing headers ensures later work doesn't try to use the old parent
      String b3 = GETTER.get(m, "b3");
      tracing.tracer().currentSpanCustomizer().tag("b3", String.valueOf(b3 != null));
    });

    String parentId = "463ac35c9f6413ad";
    producer.setProperty("b3", parentId + "-" + parentId + "-1");
    send.run();

    Span consumerSpan = takeSpan(), listenerSpan = takeSpan();
    assertThat(consumerSpan.parentId()).isEqualTo(parentId);
    assertThat(listenerSpan.parentId()).isEqualTo(consumerSpan.id());
    assertThat(listenerSpan.tags())
      .hasSize(1) // no redundant copy of consumer tags
      .containsEntry("b3", "false"); // b3 header not leaked to listener
  }

  @Test public void receive_startsNewTrace_whenFlagEnabled() throws Exception {
    MessagingTracing messagingTracing = MessagingTracing.newBuilder(tracing)
      .newTraceOnReceive(true)
      .build();
    jmsTracing = JmsTracing.newBuilder(messagingTracing).build();
    setupTracedConsumer(jmsTracing);
    producer = tracedContext.createProducer();
    producer.send(jms.queue, "foo");
    consumer.receive();
    Span producerSpan = takeSpan(), consumerSpan = takeSpan();
    assertThat(producerSpan.traceId()).isNotEqualTo(consumerSpan.traceId());
    assertThat(consumerSpan.name()).isEqualTo("receive");
    assertThat(consumerSpan.parentId()).isNull(); // root span
    assertThat(consumerSpan.kind()).isEqualTo(Span.Kind.CONSUMER);
    assertThat(consumerSpan.tags()).containsEntry("jms.queue", jms.queueName);
  }

  @Test public void receive_startsNewTrace() throws Exception {
    receive_startsNewTrace(() -> producer.send(jms.queue, "foo"));
  }

  @Test public void receive_startsNewTrace_bytes() throws Exception {
    receive_startsNewTrace(() -> producer.send(jms.queue, new byte[] {1, 2, 3, 4}));
  }

  void receive_startsNewTrace(Runnable send) throws InterruptedException {
    send.run();
    consumer.receive();
    Span consumerSpan = takeSpan();
    assertThat(consumerSpan.name()).isEqualTo("receive");
    assertThat(consumerSpan.parentId()).isNull(); // root span
    assertThat(consumerSpan.kind()).isEqualTo(Span.Kind.CONSUMER);
    assertThat(consumerSpan.tags()).containsEntry("jms.queue", jms.queueName);
  }

  @Test public void receive_resumesTrace() throws Exception {
    receiveResumesTrace(() -> producer.send(jms.queue, "foo"));
  }

  @Test public void receive_resumesTrace_bytes() throws Exception {
    receiveResumesTrace(() -> producer.send(jms.queue, new byte[] {1, 2, 3, 4}));
  }

  void receiveResumesTrace(Runnable send) throws InterruptedException, JMSException {
    String parentId = "463ac35c9f6413ad";
    producer.setProperty("b3", parentId + "-" + parentId + "-1");
    send.run();

    Message received = consumer.receive();
    Span consumerSpan = takeSpan();
    assertThat(consumerSpan.parentId()).isEqualTo(parentId);

    assertThat(received.getStringProperty("b3"))
      .isEqualTo(parentId + "-" + consumerSpan.id() + "-1");
  }

  @Test public void receive_customSampler() throws Exception {
    setupTracedConsumer(JmsTracing.create(MessagingTracing.newBuilder(tracing)
      .consumerSampler(MessagingRuleSampler.newBuilder()
        .putRule(operationEquals("receive"), Sampler.NEVER_SAMPLE)
        .build()).build()));

    producer.send(jms.queue, "foo");

    // Check that the message headers are not sampled
    assertThat(consumer.receive().getStringProperty("b3"))
      .endsWith("-0");

    // @After will also check that the consumer was not sampled
  }
}
