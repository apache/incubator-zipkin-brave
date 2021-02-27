package brave.spring.messaging;

import brave.Span;
import brave.SpanCustomizer;
import brave.Tracing;
import brave.propagation.ThreadLocalSpan;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.ChannelInterceptorAdapter;
import org.springframework.messaging.support.ExecutorChannelInterceptor;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.messaging.support.MessageHeaderAccessor;

import static brave.spring.messaging.MessageHeaderPropagation.removeAnyTraceHeaders;

/**
 * This starts and propagates {@link Span.Kind#PRODUCER} span for each message sent (via native
 * headers. It also extracts or creates a {@link Span.Kind#CONSUMER} span for each message
 * received. This span is injected onto each message so it becomes the parent when a handler later
 * calls {@link MessageHandler#handleMessage(Message)}, or a another processing library calls {@link #nextSpan(Message)}.
 *
 * <p>This implementation uses {@link ThreadLocalSpan} to propagate context between callbacks. This
 * is an alternative to {@code ThreadStatePropagationChannelInterceptor} which is less sensitive
 * to message manipulation by other interceptors.
 */
public final class TracingChannelInterceptor extends ChannelInterceptorAdapter implements
    ExecutorChannelInterceptor {

  public static TracingChannelInterceptor create(Tracing tracing) {
    return new TracingChannelInterceptor(tracing);
  }

  final Tracing tracing;
  final ThreadLocalSpan threadLocalSpan;
  final TraceContext.Injector<MessageHeaderAccessor> injector;
  final TraceContext.Extractor<MessageHeaderAccessor> extractor;

  @Autowired TracingChannelInterceptor(Tracing tracing) {
    this.tracing = tracing;
    this.threadLocalSpan = ThreadLocalSpan.create(tracing.tracer());
    this.injector = tracing.propagation().injector(MessageHeaderPropagation.INSTANCE);
    this.extractor = tracing.propagation().extractor(MessageHeaderPropagation.INSTANCE);
  }

  /**
   * Use this to create a span for processing the given message. Note: the result has no name and is
   * not started.
   *
   * <p>This creates a child from identifiers extracted from the message headers, or a new span if
   * one couldn't be extracted.
   */
  public Span nextSpan(Message<?> message) {
    MessageHeaderAccessor headers = mutableHeaderAccessor(message);
    TraceContextOrSamplingFlags extracted = extractor.extract(headers);
    Span result = tracing.tracer().nextSpan(extracted);
    if (extracted.context() == null && !result.isNoop()) {
      addTags(message, result);
    }
    return result;
  }

  /** Starts and propagates {@link Span.Kind#PRODUCER} span for each message sent. */
  @Override public Message<?> preSend(Message<?> message, MessageChannel channel) {
    MessageHeaderAccessor headers = mutableHeaderAccessor(message);
    TraceContextOrSamplingFlags extracted = extractor.extract(headers);
    Span span = threadLocalSpan.next(extracted);

    removeAnyTraceHeaders(headers, tracing.propagation().keys());
    injector.inject(span.context(), headers);
    if (!span.isNoop()) {
      span.kind(Span.Kind.PRODUCER).name("send").start();
      addTags(message, span);
    }
    return new GenericMessage<>(message.getPayload(), headers.getMessageHeaders());
  }

  @Override
  public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent,
      Exception ex) {
    finishSpan(ex);
  }

  /**
   * This starts a consumer span as a child of the incoming message or the current trace context,
   * placing it in scope until the receive completes.
   */
  @Override public Message<?> postReceive(Message<?> message, MessageChannel channel) {
    MessageHeaderAccessor headers = mutableHeaderAccessor(message);
    TraceContextOrSamplingFlags extracted = extractor.extract(headers);
    Span span = threadLocalSpan.next(extracted);

    removeAnyTraceHeaders(headers, tracing.propagation().keys());
    injector.inject(span.context(), headers);
    if (!span.isNoop()) {
      span.kind(Span.Kind.CONSUMER).name("receive").start();
      addTags(message, span);
    }
    return new GenericMessage<>(message.getPayload(), headers.getMessageHeaders());
  }

  @Override
  public void afterReceiveCompletion(Message<?> message, MessageChannel channel, Exception ex) {
    finishSpan(ex);
  }

  /**
   * This starts a span as a child of the incoming message or the current trace context.
   *
   * <p>We don't set {@link Span#kind(Span.Kind)} as it isn't likely the direct ancestor is a
   * remote message producer.
   */
  @Override public Message<?> beforeHandle(Message<?> message, MessageChannel channel,
      MessageHandler handler) {
    MessageHeaderAccessor headers = mutableHeaderAccessor(message);
    TraceContextOrSamplingFlags extracted = extractor.extract(headers);
    // remove any trace headers, but don't re-inject as we are synchronously processing the
    // message and can rely on normal scoping to access this span later.
    removeAnyTraceHeaders(headers, tracing.propagation().keys());

    // create and scope a span for the message handler
    threadLocalSpan.next(extracted).name("handle").start();
    // TODO: decide what tags if any to add here

    return new GenericMessage<>(message.getPayload(), headers.getMessageHeaders());
  }

  @Override public void afterMessageHandled(Message<?> message, MessageChannel channel,
      MessageHandler handler, Exception ex) {
    finishSpan(ex);
  }

  /** When an upstream context was not present, lookup keys are unlikely added */
  static void addTags(Message<?> message, SpanCustomizer result) {
    // TODO topic etc
  }

  void finishSpan(Exception error) {
    Span span = threadLocalSpan.remove();
    if (span == null || span.isNoop()) return;
    if (error != null) { // an error occurred, adding error to span
      String message = error.getMessage();
      if (message == null) message = error.getClass().getSimpleName();
      span.tag("error", message);
    }
    span.finish();
  }

  private MessageHeaderAccessor mutableHeaderAccessor(Message<?> message) {
    MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
    headers.setLeaveMutable(true);
    return headers;
  }
}
