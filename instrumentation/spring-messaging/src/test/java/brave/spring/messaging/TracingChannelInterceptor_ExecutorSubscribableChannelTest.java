package brave.spring.messaging;

import brave.Tracing;
import brave.propagation.StrictCurrentTraceContext;
import brave.spring.messaging.SelectableChannelInterceptor.Mode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.messaging.support.NativeMessageHeaderAccessor.NATIVE_HEADERS;

public class TracingChannelInterceptor_ExecutorSubscribableChannelTest implements MessageHandler {

  List<Span> spans = new ArrayList<>();
  SelectableChannelInterceptor interceptor = new SelectableChannelInterceptor(
      TracingChannelInterceptor.create(Tracing.newBuilder()
          .currentTraceContext(new StrictCurrentTraceContext())
          .spanReporter(spans::add)
          .build()));

  ExecutorSubscribableChannel channel = new ExecutorSubscribableChannel();
  List<Message<?>> messages = new ArrayList<>();

  @Before public void setup() {
    channel.addInterceptor(interceptor);
    channel.subscribe(this);
  }

  @Override public void handleMessage(Message<?> message) throws MessagingException {
    messages.add(message);
  }

  @Test public void startsAndStopsASpan() {
    interceptor.mode = Mode.HANDLE;

    channel.send(MessageBuilder.withPayload("foo").build());

    assertThat(spans).hasSize(1);
    assertThat(spans.get(0).kind())
        .isNull();
    assertThat(spans.get(0).name())
        .isEqualTo("handle");
  }

  /**
   * The subscriber consumes a message then synchronously processes it. Since we only inject trace
   * IDs on unprocessed messages, we remove IDs to prevent accidental re-use of the same span.
   */
  @Test public void subscriber_removesTraceIdsFromMessage() {
    channel.send(MessageBuilder.withPayload("foo").build());

    assertThat(messages.get(0).getHeaders())
        .doesNotContainKeys("X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled");
  }

  @Test public void subscriber_removesTraceIdsFromMessage_nativeHeaders() {
    channel.send(MessageBuilder.withPayload("foo").build());

    assertThat((Map) messages.get(0).getHeaders().get(NATIVE_HEADERS))
        .doesNotContainKeys("X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled");
  }

  @Test public void integrated_sendAndHandle() {
    channel.send(MessageBuilder.withPayload("foo").build());

    assertThat(spans)
        .flatExtracting(Span::kind)
        .containsExactly(null, Span.Kind.PRODUCER);
  }

  @After public void close() {
    assertThat(Tracing.current().currentTraceContext().get()).isNull();
    Tracing.current().close();
  }
}
