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
package brave.jms;

import brave.messaging.MessagingRuleSampler;
import brave.messaging.MessagingTracing;
import brave.propagation.B3SingleFormat;
import brave.propagation.SamplingFlags;
import brave.propagation.TraceContext;
import brave.sampler.Sampler;
import java.util.Collections;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import org.junit.After;
import org.junit.Before;
import org.junit.ComparisonFailure;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import zipkin2.Span;

import static brave.jms.MessagePropagation.GETTER;
import static brave.jms.MessagePropagation.SETTER;
import static brave.messaging.MessagingRequestMatchers.channelNameEquals;
import static org.assertj.core.api.Assertions.assertThat;

/** When adding tests here, also add to {@link brave.jms.ITTracingJMSConsumer} */
public class ITJms_1_1_TracingMessageConsumer extends ITJms {
  @Rule public TestName testName = new TestName();
  @Rule public JmsTestRule jms = newJmsTestRule(testName);

  Session tracedSession;
  MessageProducer messageProducer;
  MessageConsumer messageConsumer;

  QueueSession tracedQueueSession;
  QueueSender queueSender;
  QueueReceiver queueReceiver;

  TopicSession tracedTopicSession;
  TopicPublisher topicPublisher;
  TopicSubscriber topicSubscriber;

  JmsTestRule newJmsTestRule(TestName testName) {
    return new JmsTestRule.ActiveMQ(testName);
  }

  TextMessage message;
  BytesMessage bytesMessage;

  @Before public void setup() throws Exception {
    tracedSession = jmsTracing.connection(jms.connection)
      .createSession(false, Session.AUTO_ACKNOWLEDGE);
    tracedQueueSession = jmsTracing.queueConnection(jms.queueConnection)
      .createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
    tracedTopicSession = jmsTracing.topicConnection(jms.topicConnection)
      .createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

    messageProducer = jms.session.createProducer(jms.destination);
    messageConsumer = tracedSession.createConsumer(jms.destination);

    queueSender = jms.queueSession.createSender(jms.queue);
    queueReceiver = tracedQueueSession.createReceiver(jms.queue);

    topicPublisher = jms.topicSession.createPublisher(jms.topic);
    topicSubscriber = tracedTopicSession.createSubscriber(jms.topic);

    message = jms.newMessage("foo");
    bytesMessage = jms.newBytesMessage("foo");
    lockMessages();
  }

  void lockMessages() throws Exception {
    // this forces us to handle JMS write concerns!
    jms.setReadOnlyProperties(message, true);
    jms.setReadOnlyProperties(bytesMessage, true);
    bytesMessage.reset();
  }

  TraceContext resetB3PropertyWithNewSampledContext(JmsTestRule jms) throws Exception {
    TraceContext parent = newTraceContext(SamplingFlags.SAMPLED);
    message = jms.newMessage("foo");
    bytesMessage = jms.newBytesMessage("foo");
    String b3 = B3SingleFormat.writeB3SingleFormatWithoutParentId(parent);
    SETTER.put(message, "b3", b3);
    SETTER.put(bytesMessage, "b3", b3);
    lockMessages();
    return parent;
  }

  @After public void tearDownTraced() throws JMSException {
    tracedSession.close();
    tracedQueueSession.close();
    tracedTopicSession.close();
  }

  @Test public void messageListener_runsAfterConsumer() throws Exception {
    messageListener_runsAfterConsumer(() -> messageProducer.send(message), messageConsumer);
  }

  @Test public void messageListener_runsAfterConsumer_queue() throws Exception {
    messageListener_runsAfterConsumer(() -> queueSender.send(message), queueReceiver);
  }

  @Test public void messageListener_runsAfterConsumer_topic() throws Exception {
    messageListener_runsAfterConsumer(() -> topicPublisher.send(message), topicSubscriber);
  }

  void messageListener_runsAfterConsumer(JMSRunnable send, MessageConsumer messageConsumer) throws Exception {
    messageConsumer.setMessageListener(m -> {
    });
    send.run();

    Span consumerSpan = takeRemoteSpan(Span.Kind.CONSUMER), listenerSpan = takeLocalSpan();
    assertChildOf(listenerSpan, consumerSpan);
    assertSequential(consumerSpan, listenerSpan);
  }

  @Test public void messageListener_startsNewTrace() throws Exception {
    messageListener_startsNewTrace(
      () -> messageProducer.send(message),
      messageConsumer,
      Collections.singletonMap("jms.queue", jms.destinationName)
    );
  }

  @Test public void messageListener_startsNewTrace_bytes() throws Exception {
    messageListener_startsNewTrace(
      () -> messageProducer.send(bytesMessage),
      messageConsumer,
      Collections.singletonMap("jms.queue", jms.destinationName)
    );
  }

  @Test public void messageListener_startsNewTrace_queue() throws Exception {
    messageListener_startsNewTrace(
      () -> queueSender.send(message),
      queueReceiver,
      Collections.singletonMap("jms.queue", jms.queueName)
    );
  }

  @Test public void messageListener_startsNewTrace_topic() throws Exception {
    messageListener_startsNewTrace(
      () -> topicPublisher.send(message),
      topicSubscriber,
      Collections.singletonMap("jms.topic", jms.topicName)
    );
  }

  void messageListener_startsNewTrace(JMSRunnable send, MessageConsumer messageConsumer,
    Map<String, String> consumerTags) throws Exception {
    messageConsumer.setMessageListener(
      m -> {
        tracing.tracer().currentSpanCustomizer().name("message-listener");

        // clearing headers ensures later work doesn't try to use the old parent
        String b3 = GETTER.get(m, "b3");
        tracing.tracer().currentSpanCustomizer().tag("b3", String.valueOf(b3 != null));
      }
    );
    send.run();

    Span consumerSpan = takeRemoteSpan(Span.Kind.CONSUMER), listenerSpan = takeLocalSpan();

    assertThat(consumerSpan.name()).isEqualTo("receive");
    assertThat(consumerSpan.parentId()).isNull(); // root span
    assertThat(consumerSpan.tags()).isEqualTo(consumerTags);

    assertChildOf(listenerSpan, consumerSpan);
    assertThat(listenerSpan.name()).isEqualTo("message-listener"); // overridden name
    assertThat(listenerSpan.tags())
      .hasSize(1) // no redundant copy of consumer tags
      .containsEntry("b3", "false"); // b3 header not leaked to listener
  }

  @Test public void messageListener_resumesTrace() throws Exception {
    messageListener_resumesTrace(() -> messageProducer.send(message), messageConsumer);
  }

  @Test public void messageListener_resumesTrace_bytes() throws Exception {
    messageListener_resumesTrace(() -> messageProducer.send(bytesMessage), messageConsumer);
  }

  @Test public void messageListener_resumesTrace_queue() throws Exception {
    messageListener_resumesTrace(() -> queueSender.send(message), queueReceiver);
  }

  @Test public void messageListener_resumesTrace_topic() throws Exception {
    messageListener_resumesTrace(() -> topicPublisher.send(message), topicSubscriber);
  }

  void messageListener_resumesTrace(JMSRunnable send, MessageConsumer messageConsumer)
    throws Exception {
    messageConsumer.setMessageListener(m -> {
        // clearing headers ensures later work doesn't try to use the old parent
        String b3 = GETTER.get(m, "b3");
        tracing.tracer().currentSpanCustomizer().tag("b3", String.valueOf(b3 != null));
      }
    );

    TraceContext parent = resetB3PropertyWithNewSampledContext(jms);
    send.run();

    Span consumerSpan = takeRemoteSpan(Span.Kind.CONSUMER), listenerSpan = takeLocalSpan();
    assertChildOf(consumerSpan, parent);
    assertChildOf(listenerSpan, consumerSpan);
    assertThat(listenerSpan.tags())
      .hasSize(1) // no redundant copy of consumer tags
      .containsEntry("b3", "false"); // b3 header not leaked to listener
  }

  @Test public void receive_startsNewTrace() throws Exception {
    receive_startsNewTrace(
      () -> messageProducer.send(message),
      messageConsumer,
      Collections.singletonMap("jms.queue", jms.destinationName)
    );
  }

  @Test public void receive_startsNewTrace_queue() throws Exception {
    receive_startsNewTrace(
      () -> queueSender.send(message),
      queueReceiver,
      Collections.singletonMap("jms.queue", jms.queueName)
    );
  }

  @Test public void receive_startsNewTrace_topic() throws Exception {
    receive_startsNewTrace(
      () -> topicPublisher.send(message),
      topicSubscriber,
      Collections.singletonMap("jms.topic", jms.topicName)
    );
  }

  void receive_startsNewTrace(JMSRunnable send, MessageConsumer messageConsumer,
    Map<String, String> consumerTags) throws Exception {
    send.run();

    messageConsumer.receive();

    Span consumerSpan = takeRemoteSpan(Span.Kind.CONSUMER);
    assertThat(consumerSpan.name()).isEqualTo("receive");
    assertThat(consumerSpan.parentId()).isNull(); // root span
    assertThat(consumerSpan.tags()).isEqualTo(consumerTags);
  }

  @Test public void receive_resumesTrace() throws Exception {
    receive_resumesTrace(() -> messageProducer.send(message), messageConsumer);
  }

  @Test //(expected = ComparisonFailure.class) // TODO: https://github.com/openzipkin/brave/issues/967
  public void receive_resumesTrace_bytes() throws Exception {
    receive_resumesTrace(() -> messageProducer.send(bytesMessage), messageConsumer);
  }

  @Test public void receive_resumesTrace_queue() throws Exception {
    receive_resumesTrace(() -> queueSender.send(message), queueReceiver);
  }

  @Test public void receive_resumesTrace_topic() throws Exception {
    receive_resumesTrace(() -> topicPublisher.send(message), topicSubscriber);
  }

  void receive_resumesTrace(JMSRunnable send, MessageConsumer messageConsumer) throws Exception {
    TraceContext parent = resetB3PropertyWithNewSampledContext(jms);
    send.run();

    Message received = messageConsumer.receive();
    Span consumerSpan = takeRemoteSpan(Span.Kind.CONSUMER);
    assertChildOf(consumerSpan, parent);

    assertThat(received.getStringProperty("b3"))
      .isEqualTo(parent.traceIdString() + "-" + consumerSpan.id() + "-1");
  }

  @Test public void receive_customSampler() throws Exception {
    queueReceiver.close();
    tracedSession.close();

    MessagingRuleSampler consumerSampler = MessagingRuleSampler.newBuilder()
      .putRule(channelNameEquals(jms.queue.getQueueName()), Sampler.NEVER_SAMPLE)
      .build();

    try (MessagingTracing messagingTracing = MessagingTracing.newBuilder(tracing)
      .consumerSampler(consumerSampler)
      .build()) {

      // JMS 1.1 didn't have auto-closeable. tearDownTraced closes these
      tracedQueueSession = JmsTracing.create(messagingTracing)
        .queueConnection(jms.queueConnection)
        .createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      messageConsumer = tracedQueueSession.createConsumer(jms.queue);

      queueSender.send(message);

      // Check that the message headers are not sampled
      assertThat(messageConsumer.receive().getStringProperty("b3"))
        .endsWith("-0");
    }

    // @After will also check that the consumer was not sampled
  }
}
