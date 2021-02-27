package brave.spring.messaging;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.ExecutorChannelInterceptor;

final class SelectableChannelInterceptor implements ChannelInterceptor, ExecutorChannelInterceptor {
  enum Mode {
    SEND,
    RECEIVE,
    HANDLE,
    ALL
  }

  final ChannelInterceptor delegate;
  volatile Mode mode;

  SelectableChannelInterceptor(ChannelInterceptor delegate) {
    this.delegate = delegate;
    this.mode = Mode.ALL;
  }

  @Override public Message<?> preSend(Message<?> message, MessageChannel channel) {
    switch (mode) {
      case SEND:
      case ALL:
        return delegate.preSend(message, channel);
      default:
    }
    return message;
  }

  @Override public void postSend(Message<?> message, MessageChannel channel, boolean sent) {
    switch (mode) {
      case SEND:
      case ALL:
        delegate.postSend(message, channel, sent);
        break;
      default:
    }
  }

  @Override
  public void afterSendCompletion(Message<?> message, MessageChannel channel, boolean sent,
      Exception ex) {
    switch (mode) {
      case SEND:
      case ALL:
        delegate.afterSendCompletion(message, channel, sent, ex);
        break;
      default:
    }
  }

  @Override public boolean preReceive(MessageChannel channel) {
    switch (mode) {
      case RECEIVE:
      case ALL:
        return delegate.preReceive(channel);
      default:
    }
    return true;
  }

  @Override public Message<?> postReceive(Message<?> message, MessageChannel channel) {
    switch (mode) {
      case RECEIVE:
      case ALL:
        return delegate.postReceive(message, channel);
      default:
    }
    return message;
  }

  @Override
  public void afterReceiveCompletion(Message<?> message, MessageChannel channel, Exception ex) {
    switch (mode) {
      case RECEIVE:
      case ALL:
        delegate.afterReceiveCompletion(message, channel, ex);
      default:
    }
  }

  @Override public Message<?> beforeHandle(Message<?> message, MessageChannel channel,
      MessageHandler handler) {
    switch (mode) {
      case HANDLE:
      case ALL:
        return ((ExecutorChannelInterceptor) delegate).beforeHandle(message, channel, handler);
      default:
    }
    return message;
  }

  @Override public void afterMessageHandled(Message<?> message, MessageChannel channel,
      MessageHandler handler, Exception ex) {
    switch (mode) {
      case HANDLE:
      case ALL:
        ((ExecutorChannelInterceptor) delegate).afterMessageHandled(message, channel, handler, ex);
      default:
    }
  }
}
