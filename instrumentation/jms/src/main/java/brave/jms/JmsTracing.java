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

import brave.Span;
import brave.SpanCustomizer;
import brave.Tracer;
import brave.Tracing;
import brave.internal.Nullable;
import brave.messaging.MessagingRequest;
import brave.messaging.MessagingTracing;
import brave.propagation.Propagation;
import brave.propagation.TraceContext.Extractor;
import brave.propagation.TraceContext.Injector;
import brave.propagation.TraceContextOrSamplingFlags;
import brave.sampler.SamplerFunction;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
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

import static brave.internal.Throwables.propagateIfFatal;
import static brave.jms.MessageParser.destination;
import static brave.jms.MessagePropagation.GETTER;

/** Use this class to decorate your JMS consumer / producer and enable Tracing. */
public final class JmsTracing {
  static final String JMS_QUEUE = "jms.queue";
  static final String JMS_TOPIC = "jms.topic";

  // Use nested class to ensure logger isn't initialized unless it is accessed once.
  private static final class LoggerHolder {
    static final Logger LOG = Logger.getLogger(JmsTracing.class.getName());
  }

  // Use nested class to ensure we only check once per classloader
  private static final class JmsTypes {
    static final boolean HAS_JMS_PRODUCER = hasJMSProducer();

    static boolean hasJMSProducer() {
      try {
        Class.forName("javax.jms.JMSProducer");
        return true; // intentionally doesn't not access the type prior to the above guard
      } catch (Throwable t) {
        propagateIfFatal(t);
        return false;
      }
    }
  }

  public static JmsTracing create(Tracing tracing) {
    return newBuilder(tracing).build();
  }

  /** @since 5.9 */
  public static JmsTracing create(MessagingTracing messagingTracing) {
    return newBuilder(messagingTracing).build();
  }

  public static Builder newBuilder(Tracing tracing) {
    return newBuilder(MessagingTracing.create(tracing));
  }

  /** @since 5.9 */
  public static Builder newBuilder(MessagingTracing messagingTracing) {
    return new Builder(messagingTracing);
  }

  public static final class Builder {
    final MessagingTracing messagingTracing;
    String remoteServiceName = "jms";

    Builder(MessagingTracing messagingTracing) {
      if (messagingTracing == null) throw new NullPointerException("messagingTracing == null");
      this.messagingTracing = messagingTracing;
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

  final Tracing tracing;
  final MessagingTracing messagingTracing;
  final Tracer tracer;
  final Extractor<MessageProducerRequest> messageProducerExtractor;
  final Injector<MessageProducerRequest> messageProducerInjector;
  final Extractor<MessageConsumerRequest> messageConsumerExtractor;
  final Injector<MessageConsumerRequest> messageConsumerInjector;
  final Extractor<Message> processorExtractor;
  final SamplerFunction<MessagingRequest> producerSampler, consumerSampler;
  final String remoteServiceName;
  final Set<String> propagationKeys;

  // raw types to avoid accessing JMS 2.0 types unless we are sure they are present
  // Caching here instead of deferring further as there is overhead creating extractors and
  // injectors, particularly when decorated with extra fields or secondary sampling.
  @Nullable final Extractor jmsProducerExtractor;
  @Nullable final Injector jmsProducerInjector;

  JmsTracing(Builder builder) { // intentionally hidden constructor
    this.tracing = builder.messagingTracing.tracing();
    this.tracer = tracing.tracer();
    this.messagingTracing = builder.messagingTracing;
    Propagation<String> propagation = tracing.propagation();
    if (JmsTypes.HAS_JMS_PRODUCER) {
      this.jmsProducerExtractor = propagation.extractor(JMSProducerRequest.GETTER);
      this.jmsProducerInjector = propagation.injector(JMSProducerRequest.SETTER);
    } else {
      this.jmsProducerExtractor = null;
      this.jmsProducerInjector = null;
    }
    this.messageProducerExtractor = propagation.extractor(MessageProducerRequest.GETTER);
    this.messageProducerInjector = propagation.injector(MessageProducerRequest.SETTER);
    this.messageConsumerExtractor = propagation.extractor(MessageConsumerRequest.GETTER);
    this.messageConsumerInjector = propagation.injector(MessageConsumerRequest.SETTER);
    this.processorExtractor = tracing.propagation().extractor(GETTER);
    this.producerSampler = builder.messagingTracing.producerSampler();
    this.consumerSampler = builder.messagingTracing.consumerSampler();
    this.remoteServiceName = builder.remoteServiceName;
    this.propagationKeys = new LinkedHashSet<>(tracing.propagation().keys());
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
   * <p>This creates a child from identifiers extracted from the message properties, or a new span
   * if one couldn't be extracted.
   */
  public Span nextSpan(Message message) {
    TraceContextOrSamplingFlags extracted =
      extractAndClearProperties(processorExtractor, message, message);
    Span result = tracer.nextSpan(extracted); // Processor spans use the normal sampler.

    // When an upstream context was not present, lookup keys are unlikely added
    if (extracted.context() == null && !result.isNoop()) {
      // simplify code by re-using an existing MessagingRequest impl
      tagQueueOrTopic(new MessageConsumerRequest(message, destination(message)), result);
    }
    return result;
  }

  <R> TraceContextOrSamplingFlags extractAndClearProperties(
    Extractor<R> extractor, R request, Message message
  ) {
    TraceContextOrSamplingFlags extracted = extractor.extract(request);
    // Clear propagation regardless of extraction as JMS requires clearing as a means to make the
    // message writable
    PropertyFilter.filterProperties(message, propagationKeys);
    return extracted;
  }

  /** Creates a potentially noop remote span representing this request */
  Span nextMessagingSpan(
    SamplerFunction<MessagingRequest> sampler,
    MessagingRequest request,
    TraceContextOrSamplingFlags extracted
  ) {
    Boolean sampled = extracted.sampled();
    // only recreate the context if the messaging sampler made a decision
    if (sampled == null && (sampled = sampler.trySample(request)) != null) {
      extracted = extracted.sampled(sampled.booleanValue());
    }
    return tracer.nextSpan(extracted);
  }
  Span newMessagingTrace(
    SamplerFunction<MessagingRequest> sampler,
    MessagingRequest request,
    TraceContextOrSamplingFlags extracted
  ) {
    String traceId = null, spanId = null;
    if (extracted.context() != null) {
      traceId = extracted.context().traceIdString();
      if (extracted.context().spanIdString() != null) {
        spanId = extracted.context().spanIdString();
      }
    }
    Boolean sampled = extracted.sampled();
    if (sampled == null && (sampled = sampler.trySample(request)) != null) {
      extracted = extracted.sampled(sampled.booleanValue());
    }
    Span span;
    if (extracted.samplingFlags() != null) {
      extracted = TraceContextOrSamplingFlags.create(extracted.samplingFlags());
      span = tracer.nextSpan(extracted);
    } else {
      span = tracer.newTrace();
    }
    if (traceId != null) span.tag("parent.trace_id", traceId);
    if (spanId != null) span.tag("parent.span_id", spanId);
    return span;
  }

  void tagQueueOrTopic(MessagingRequest request, SpanCustomizer span) {
    String channelName = request.channelName();
    if (channelName == null) return;

    String channelKind = request.channelKind();
    if ("queue".equals(channelKind)) {
      span.tag(JMS_QUEUE, channelName);
    } else if ("topic".equals(channelKind)) {
      span.tag(JMS_TOPIC, channelName);
    }
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
   *  } catch (Throwable t) {
   *    Call.propagateIfFatal(e);
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
