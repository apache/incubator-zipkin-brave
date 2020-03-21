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
import javax.jms.Session;
import javax.jms.TextMessage;
import org.junit.Test;
import org.junit.rules.TestName;
import zipkin2.Span;

import static brave.messaging.MessagingRequestMatchers.channelNameEquals;
import static org.assertj.core.api.Assertions.assertThat;

/** When adding tests here, also add to {@linkplain brave.jms.ITTracingJMSConsumer} */
public class ITJms_2_0_TracingMessageConsumer extends ITJms_1_1_TracingMessageConsumer {
  @Override JmsTestRule newJmsTestRule(TestName testName) {
    return new ArtemisJmsTestRule(testName);
  }

  // Inability to encode "b3" on a received BytesMessage only applies to ActiveMQ 5.x
  @Test public void receive_resumesTrace_bytes() throws Exception {
    receive_resumesTrace(() -> messageProducer.send(bytesMessage), messageConsumer);
  }

  @Test public void receive_customSampler() throws Exception {
    queueReceiver.close();

    MessagingRuleSampler consumerSampler = MessagingRuleSampler.newBuilder()
      .putRule(channelNameEquals(jms.queue.getQueueName()), Sampler.NEVER_SAMPLE)
      .build();

    try (MessagingTracing messagingTracing = MessagingTracing.newBuilder(tracing)
      .consumerSampler(consumerSampler)
      .build();
         JMSContext context = JmsTracing.create(messagingTracing)
           .connectionFactory(((ArtemisJmsTestRule) jms).factory)
           .createContext(JMSContext.AUTO_ACKNOWLEDGE);
         JMSConsumer consumer = context.createConsumer(jms.queue)
    ) {
      queueSender.send(message);

      // Check that the message headers are not sampled
      assertThat(consumer.receive().getStringProperty("b3"))
        .endsWith("-0");
    }

    // @After will also check that the consumer was not sampled
  }
}
