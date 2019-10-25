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

import brave.messaging.ChannelAdapter;
import brave.messaging.MessageConsumerAdapter;
import brave.messaging.MessageProducerAdapter;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;

import static brave.jms.JmsTracing.JMS_QUEUE;
import static brave.jms.JmsTracing.JMS_TOPIC;

class JmsAdapter {

  static class JmsMessageConsumerAdapter implements MessageConsumerAdapter<Message> {

    final JmsTracing jmsTracing;

    JmsMessageConsumerAdapter(JmsTracing jmsTracing) {
      this.jmsTracing = jmsTracing;
    }

    static JmsMessageConsumerAdapter create(JmsTracing jmsTracing) {
      return new JmsMessageConsumerAdapter(jmsTracing);
    }

    @Override public String operation(Message message) {
      return "receive";
    }

    @Override public String identifier(Message message) {
      try {
        return message.getJMSMessageID();
      } catch (JMSException e) {
        // don't crash on wonky exceptions!
      }
      return null;
    }

    @Override public String identifierTagKey() {
      return "jms.message_id";
    }
  }

  static class JmsMessageProducerAdapter implements MessageProducerAdapter<Message> {

    final JmsTracing jmsTracing;

    JmsMessageProducerAdapter(JmsTracing jmsTracing) {
      this.jmsTracing = jmsTracing;
    }

    static JmsMessageProducerAdapter create(JmsTracing jmsTracing) {
      return new JmsMessageProducerAdapter(jmsTracing);
    }

    @Override public String operation(Message message) {
      return "send";
    }

    @Override public String identifier(Message message) {
      try {
        return message.getJMSMessageID();
      } catch (JMSException e) {
        // don't crash on wonky exceptions!
      }
      return null;
    }

    @Override public String identifierTagKey() {
      return null;
    }
  }

  static class JmsChannelAdapter implements ChannelAdapter<Destination> {

    final JmsTracing jmsTracing;

    JmsChannelAdapter(JmsTracing jmsTracing) {
      this.jmsTracing = jmsTracing;
    }

    static JmsChannelAdapter create(JmsTracing jmsTracing) {
      return new JmsChannelAdapter(jmsTracing);
    }

    @Override public String channel(Destination destination) {
      try {
        if (destination instanceof Queue) {
          return ((Queue) destination).getQueueName();
        } else if (destination instanceof Topic) {
          return ((Topic) destination).getTopicName();
        }
      } catch (JMSException ignored) {
        // don't crash on wonky exceptions!
      }
      return null;
    }

    @Override public String channelTagKey(Destination destination) {
      if (destination instanceof Queue) {
        return JMS_QUEUE;
      } else if (destination instanceof Topic) {
        return JMS_TOPIC;
      }
      return null;
    }

    @Override public String remoteServiceName(Destination message) {
      return jmsTracing.remoteServiceName;
    }
  }
}
