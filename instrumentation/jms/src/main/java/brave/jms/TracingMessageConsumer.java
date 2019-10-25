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

import brave.messaging.MessagingConsumerHandler;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.Topic;
import javax.jms.TopicPublisher;
import javax.jms.TopicSubscriber;

import static brave.jms.TracingConnection.TYPE_QUEUE;
import static brave.jms.TracingConnection.TYPE_TOPIC;

/** Implements all interfaces as according to ActiveMQ, this is typical of JMS 1.1. */
final class TracingMessageConsumer
  extends MessagingConsumerHandler<MessageConsumer, Destination, Message>
  implements QueueReceiver, TopicSubscriber {

  static TracingMessageConsumer create(MessageConsumer delegate, JmsTracing jmsTracing) {
    if (delegate instanceof TracingMessageConsumer) return (TracingMessageConsumer) delegate;
    return new TracingMessageConsumer(delegate, jmsTracing);
  }

  final JmsTracing jmsTracing;
  final int types;

  TracingMessageConsumer(MessageConsumer delegate, JmsTracing jmsTracing) {
    super(
      delegate,
      jmsTracing.msgTracing,
      jmsTracing.channelAdapter,
      jmsTracing.consumerMessageAdapter,
      jmsTracing.extractor,
      jmsTracing.injector);
    this.jmsTracing = jmsTracing;
    int types = 0;
    if (delegate instanceof QueueSender) types |= TYPE_QUEUE;
    if (delegate instanceof TopicPublisher) types |= TYPE_TOPIC;
    this.types = types;
  }

  @Override public String getMessageSelector() throws JMSException {
    return delegate.getMessageSelector();
  }

  @Override public MessageListener getMessageListener() throws JMSException {
    return delegate.getMessageListener();
  }

  @Override public void setMessageListener(MessageListener listener) throws JMSException {
    delegate.setMessageListener(TracingMessageListener.create(listener, jmsTracing));
  }

  @Override public Message receive() throws JMSException {
    Message message = delegate.receive();
    handleConsume(message.getJMSDestination(), message);
    return message;
  }

  @Override public Message receive(long timeout) throws JMSException {
    Message message = delegate.receive(timeout);
    handleConsume(message.getJMSDestination(), message);
    return message;
  }

  @Override public Message receiveNoWait() throws JMSException {
    Message message = delegate.receiveNoWait();
    handleConsume(message.getJMSDestination(), message);
    return message;
  }

  @Override public void close() throws JMSException {
    delegate.close();
  }

  // QueueReceiver

  @Override public Queue getQueue() throws JMSException {
    checkQueueReceiver();
    return ((QueueReceiver) delegate).getQueue();
  }

  void checkQueueReceiver() {
    if ((types & TYPE_QUEUE) != TYPE_QUEUE) {
      throw new IllegalStateException(delegate + " is not a QueueReceiver");
    }
  }

  // TopicSubscriber

  @Override public Topic getTopic() throws JMSException {
    checkTopicSubscriber();
    return ((TopicSubscriber) delegate).getTopic();
  }

  @Override public boolean getNoLocal() throws JMSException {
    checkTopicSubscriber();
    return ((TopicSubscriber) delegate).getNoLocal();
  }

  void checkTopicSubscriber() {
    if ((types & TYPE_TOPIC) != TYPE_TOPIC) {
      throw new IllegalStateException(delegate + " is not a TopicSubscriber");
    }
  }
}
