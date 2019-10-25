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

import brave.Span;
import brave.Tracing;
import brave.internal.Nullable;
import brave.messaging.MessagingTracing;
import brave.propagation.Propagation.Getter;
import brave.propagation.Propagation.Setter;
import brave.propagation.TraceContext;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.QueueConnection;
import javax.jms.TopicConnection;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XAQueueConnection;
import javax.jms.XATopicConnection;

import static brave.propagation.B3SingleFormat.writeB3SingleFormatWithoutParentId;

/** Use this class to decorate your Jms consumer / producer and enable Tracing. */
public final class JmsTracing {
  static final String JMS_QUEUE = "jms.queue";
  static final String JMS_TOPIC = "jms.topic";

  // Use nested class to ensure logger isn't initialized unless it is accessed once.
  private static final class LoggerHolder {
    static final Logger LOG = Logger.getLogger(JmsTracing.class.getName());
  }

  static final Getter<Message, String> GETTER = new Getter<Message, String>() {
    @Override public String get(Message message, String name) {
      try {
        return message.getStringProperty(name);
      } catch (JMSException e) {
        log(e, "error getting property {0} from message {1}", name, message);
        return null;
      }
    }

    @Override public String toString() {
      return "Message::getStringProperty";
    }
  };

  static final Setter<Message, String> SETTER = new Setter<Message, String>() {
    @Override public void put(Message message, String name, String value) {
      try {
        message.setStringProperty(name, value);
      } catch (JMSException e) {
        log(e, "error setting property {0} on message {1}", name, message);
      }
    }

    @Override public String toString() {
      return "Message::setStringProperty";
    }
  };

  public static JmsTracing create(Tracing tracing) {
    return new Builder(tracing).build();
  }

  public static Builder newBuilder(Tracing tracing) {
    return new Builder(tracing);
  }

  public static final class Builder {
    final MessagingTracing msgTracing;
    String remoteServiceName = "jms";

    Builder(Tracing tracing) {
      if (tracing == null) throw new NullPointerException("tracing == null");
      this.msgTracing = MessagingTracing.create(tracing);
    }

    Builder(MessagingTracing msgTracing) {
      if (msgTracing == null) throw new NullPointerException("msgTracing == null");
      this.msgTracing = msgTracing;
    }

    /**
     * The remote service name that describes the broker in the dependency graph. Defaults to "jms"
     */
    public Builder remoteServiceName(String remoteServiceName) {
      this.remoteServiceName = remoteServiceName;
      return this;
    }

    public JmsTracing build() {
      return new JmsTracing(this);
    }
  }

  final MessagingTracing msgTracing;
  final Extractor<Message> extractor;
  final Injector<Message> injector;
  final JmsAdapter.JmsChannelAdapter channelAdapter;
  final JmsAdapter.JmsMessageConsumerAdapter consumerMessageAdapter;
  final JmsAdapter.JmsMessageProducerAdapter producerMessageAdapter;
  final String remoteServiceName;
  final Set<String> propagationKeys;

  // Lazy init as a constant eagerly initializes hooks like log4j2 even when the tracer is no-op
  static volatile Logger logger;

  JmsTracing(Builder builder) { // intentionally hidden constructor
    this.msgTracing = builder.msgTracing;
    this.extractor = msgTracing.tracing().propagation().extractor(GETTER);
    this.remoteServiceName = builder.remoteServiceName;
    this.propagationKeys = new LinkedHashSet<>(msgTracing.tracing().propagation().keys());
    this.consumerMessageAdapter = JmsAdapter.JmsMessageConsumerAdapter.create(this);
    this.channelAdapter = JmsAdapter.JmsChannelAdapter.create(this);
    this.producerMessageAdapter = JmsAdapter.JmsMessageProducerAdapter.create(this);
    this.injector = new Injector<Message>() {
      @Override public void inject(TraceContext traceContext, Message carrier) {
        setNextParent(carrier, traceContext);
      }

      @Override
      public String toString() {
        return "Message::setStringProperty(\"b3\",singleHeaderFormatWithoutParent)";
      }
    };
  }

  public Connection connection(Connection connection) {
    // It is common to implement both interfaces
    if (connection instanceof XAConnection) {
      return xaConnection((XAConnection) connection);
    }
    return TracingConnection.create(connection, this);
  }

  public QueueConnection queueConnection(QueueConnection connection) {
    // It is common to implement both interfaces
    if (connection instanceof XAQueueConnection) {
      return xaQueueConnection((XAQueueConnection) connection);
    }
    return TracingConnection.create(connection, this);
  }

  public TopicConnection topicConnection(TopicConnection connection) {
    // It is common to implement both interfaces
    if (connection instanceof XATopicConnection) {
      return xaTopicConnection((XATopicConnection) connection);
    }
    return TracingConnection.create(connection, this);
  }

  public XAConnection xaConnection(XAConnection xaConnection) {
    return TracingXAConnection.create(xaConnection, this);
  }

  public XAQueueConnection xaQueueConnection(XAQueueConnection connection) {
    return TracingXAConnection.create(connection, this);
  }

  public XATopicConnection xaTopicConnection(XATopicConnection connection) {
    return TracingXAConnection.create(connection, this);
  }

  public ConnectionFactory connectionFactory(ConnectionFactory connectionFactory) {
    // It is common to implement both interfaces
    if (connectionFactory instanceof XAConnectionFactory) {
      return (ConnectionFactory) xaConnectionFactory((XAConnectionFactory) connectionFactory);
    }
    return TracingConnectionFactory.create(connectionFactory, this);
  }

  public XAConnectionFactory xaConnectionFactory(XAConnectionFactory xaConnectionFactory) {
    return TracingXAConnectionFactory.create(xaConnectionFactory, this);
  }

  /**
   * Returns a message listener that optionally starts a consumer span for the message received
   * before wrapping the listener in a separate span.
   *
   * @param messageListener listener to wrap
   * @param addConsumerSpan set to true when the underlying message receipt is not traced (ex. JCA)
   */
  public MessageListener messageListener(MessageListener messageListener, boolean addConsumerSpan) {
    if (messageListener instanceof TracingMessageListener) return messageListener;
    return new TracingMessageListener(messageListener, this, addConsumerSpan);
  }

  /**
   * Use this to create a span for processing the given message. Note: the result has no name and is
   * not started.
   *
   * <p>In general, prefer {@link MessageListener} for processing messages, as it is more efficient
   * and less lossy with regards to context data.
   *
   * <p>This creates a child from identifiers extracted from the message message, or a new span if
   * one couldn't be extracted.
   */
  public Span nextSpan(Message message) {
    return msgTracing.nextSpan(channelAdapter, consumerMessageAdapter, extractor, message,
      destination(message));
  }

  //TraceContextOrSamplingFlags extractAndClearMessage(Message message) {
  //  TraceContextOrSamplingFlags extracted = extractor.extract(message);
  //  // Clear propagation regardless of extraction as JMS requires clearing as a means to make the
  //  // message writable
  //  PropertyFilter.MESSAGE.filterProperties(message, propagationKeys);
  //  return extracted;
  //}

  /**
   * We currently serialize the context as a "b3" message property. We can't add the context as an
   * Object property because JMS only supports "primitive objects, String, Map and List types".
   *
   * <p>Using {@link MessageListener} is preferred where possible, as we can coordinate without
   * writing "b3", which notably saves us from losing data in {@link TraceContext#extra()}.
   */
  void setNextParent(Message message, TraceContext context) {
    addB3SingleHeader(message, context);
  }

  /** This is only safe to call after {@link JmsTracing#extractAndClearMessage(Message)} */
  static void addB3SingleHeader(Message message, TraceContext context) {
    SETTER.put(message, "b3", writeB3SingleFormatWithoutParentId(context));
  }

  @Nullable static Destination destination(Message message) {
    try {
      return message.getJMSDestination();
    } catch (JMSException e) {
      log(e, "error destination of message {0}", message, null);
    }
    return null;
  }

  /**
   * Avoids array allocation when logging a parameterized message when fine level is disabled. The
   * second parameter is optional. This is used to pinpoint provider-specific problems that throw
   * {@link JMSException} or {@link JMSRuntimeException}.
   *
   * <p>Ex.
   * <pre>{@code
   * try {
   *    return message.getStringProperty(name);
   *  } catch (JMSException e) {
   *    log(e, "error getting property {0} from message {1}", name, message);
   *    return null;
   *  }
   * }</pre>
   *
   * @param thrown the JMS exception that was caught
   * @param msg the format string
   * @param zero will end up as {@code {0}} in the format string
   * @param one if present, will end up as {@code {1}} in the format string
   */
  static void log(Throwable thrown, String msg, Object zero, @Nullable Object one) {
    Logger logger = LoggerHolder.LOG;
    if (!logger.isLoggable(Level.FINE)) return; // fine level to not fill logs
    LogRecord lr = new LogRecord(Level.FINE, msg);
    Object[] params = one != null ? new Object[] {zero, one} : new Object[] {zero};
    lr.setParameters(params);
    lr.setThrown(thrown);
    logger.log(lr);
  }
}
