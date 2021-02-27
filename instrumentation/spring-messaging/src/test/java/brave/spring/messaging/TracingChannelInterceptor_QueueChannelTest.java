package brave.spring.messaging;

import brave.Tracing;
import brave.propagation.StrictCurrentTraceContext;
import brave.spring.messaging.SelectableChannelInterceptor.Mode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.ExecutorSubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.messaging.support.NativeMessageHeaderAccessor;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.messaging.support.NativeMessageHeaderAccessor.NATIVE_HEADERS;

public class TracingChannelInterceptor_QueueChannelTest {

  List<Span> spans = new ArrayList<>();
  SelectableChannelInterceptor interceptor = new SelectableChannelInterceptor(
      TracingChannelInterceptor.create(Tracing.newBuilder()
          .currentTraceContext(new StrictCurrentTraceContext())
          .spanReporter(spans::add)
          .build()));

  QueueChannel queueChannel = new QueueChannel();

  @Before public void setup() {
    queueChannel.addInterceptor(interceptor);
  }

  @Test public void injectsProducerSpan() {
    interceptor.mode = Mode.SEND;

    queueChannel.send(MessageBuilder.withPayload("foo").build());

    assertThat(queueChannel.receive().getHeaders())
        .containsKeys("X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled", "nativeHeaders");
    assertThat(spans)
        .hasSize(1)
        .flatExtracting(Span::kind)
        .containsExactly(Span.Kind.PRODUCER);
  }

  @Test public void injectsProducerSpan_nativeHeaders() {
    interceptor.mode = Mode.SEND;

    queueChannel.send(MessageBuilder.withPayload("foo").build());

    assertThat((Map) queueChannel.receive().getHeaders().get(NATIVE_HEADERS))
        .containsOnlyKeys("X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled");
  }

  /**
   * If the producer is acting on an un-processed message (ex via a polling consumer), it should
   * look at trace headers when there is no span in scope, and use that as the parent context.
   */
  @Test public void producerConsidersOldSpanIds() {
    interceptor.mode = Mode.SEND;

    queueChannel.send(MessageBuilder.withPayload("foo")
        .setHeader("X-B3-TraceId", "000000000000000a")
        .setHeader("X-B3-ParentSpanId", "000000000000000a")
        .setHeader("X-B3-SpanId", "000000000000000b")
        .build());

    assertThat(queueChannel.receive().getHeaders())
        .containsEntry("X-B3-ParentSpanId", "000000000000000b");
  }

  @Test public void producerConsidersOldSpanIds_nativeHeaders() {
    interceptor.mode = Mode.SEND;

    NativeMessageHeaderAccessor accessor = new NativeMessageHeaderAccessor() {
    };

    accessor.setNativeHeader("X-B3-TraceId", "000000000000000a");
    accessor.setNativeHeader("X-B3-ParentSpanId", "000000000000000a");
    accessor.setNativeHeader("X-B3-SpanId", "000000000000000b");

    queueChannel.send(MessageBuilder.withPayload("foo")
        .copyHeaders(accessor.toMessageHeaders())
        .build());

    assertThat((Map) queueChannel.receive().getHeaders().get(NATIVE_HEADERS))
        .containsEntry("X-B3-ParentSpanId", Collections.singletonList("000000000000000b"));
  }

  /** We have to inject headers on a polling receive as any future processor will come later */
  @Test public void pollingReceive_injectsConsumerSpan() {
    interceptor.mode = Mode.RECEIVE;

    queueChannel.send(MessageBuilder.withPayload("foo").build());

    assertThat(queueChannel.receive().getHeaders())
        .containsKeys("X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled", "nativeHeaders");
    assertThat(spans)
        .hasSize(1)
        .flatExtracting(Span::kind)
        .containsExactly(Span.Kind.CONSUMER);
  }

  @Test public void pollingReceive_injectsConsumerSpan_nativeHeaders() {
    interceptor.mode = Mode.RECEIVE;

    queueChannel.send(MessageBuilder.withPayload("foo").build());

    assertThat((Map) queueChannel.receive().getHeaders().get(NATIVE_HEADERS))
        .containsOnlyKeys("X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled");
  }

  @Test public void subscriber_startsAndStopsProcessingSpan() {
    ExecutorSubscribableChannel channel = new ExecutorSubscribableChannel();
    channel.addInterceptor(interceptor);
    interceptor.mode = Mode.HANDLE;

    List<Message<?>> messages = new ArrayList<>();
    channel.subscribe(messages::add);

    channel.send(MessageBuilder.withPayload("foo").build());

    assertThat(messages.get(0).getHeaders())
        .doesNotContainKeys("X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled", "nativeHeaders");
    assertThat(spans.get(0).kind())
        .isNull();
  }

  /**
   * The subscriber consumes a message then synchronously processes it. Since we only inject trace
   * IDs on unprocessed messages, we remove IDs to prevent accidental re-use of the same span.
   */
  @Test public void subscriber_removesTraceIdsFromMessage() {
    ExecutorSubscribableChannel channel = new ExecutorSubscribableChannel();
    channel.addInterceptor(interceptor);

    List<Message<?>> messages = new ArrayList<>();
    channel.subscribe(messages::add);

    channel.send(MessageBuilder.withPayload("foo").build());

    assertThat(messages.get(0).getHeaders())
        .doesNotContainKeys("X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled");
  }

  @Test public void subscriber_removesTraceIdsFromMessage_nativeHeaders() {
    ExecutorSubscribableChannel channel = new ExecutorSubscribableChannel();
    channel.addInterceptor(interceptor);

    List<Message<?>> messages = new ArrayList<>();
    channel.subscribe(messages::add);

    channel.send(MessageBuilder.withPayload("foo").build());

    assertThat((Map) messages.get(0).getHeaders().get(NATIVE_HEADERS))
        .doesNotContainKeys("X-B3-TraceId", "X-B3-SpanId", "X-B3-Sampled");
  }

  @Test public void integrated_sendAndPoll() {
    queueChannel.send(MessageBuilder.withPayload("foo").build());
    queueChannel.receive();

    assertThat(spans)
        .flatExtracting(Span::kind)
        .containsExactlyInAnyOrder(Span.Kind.CONSUMER, Span.Kind.PRODUCER);
  }

  @Test public void integrated_sendAndSubscriber() {
    ExecutorSubscribableChannel channel = new ExecutorSubscribableChannel();
    channel.addInterceptor(interceptor);

    List<Message<?>> messages = new ArrayList<>();
    channel.subscribe(messages::add);

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
