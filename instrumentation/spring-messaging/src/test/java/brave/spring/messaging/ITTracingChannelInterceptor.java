package brave.spring.messaging;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import brave.propagation.StrictCurrentTraceContext;
import brave.spring.messaging.SelectableChannelInterceptor.Mode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.config.GlobalChannelInterceptor;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.ChannelInterceptor;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ported from org.springframework.cloud.sleuth.instrument.messaging.TraceChannelInterceptorTest to
 * allow sleuth to decommission its implementation.
 */
@SpringBootTest(
    classes = ITTracingChannelInterceptor.App.class,
    webEnvironment = SpringBootTest.WebEnvironment.NONE
)
@RunWith(SpringRunner.class)
@DirtiesContext
public class ITTracingChannelInterceptor implements MessageHandler {

  @Autowired SelectableChannelInterceptor interceptor;

  @Autowired @Qualifier("directChannel") DirectChannel directChannel;

  @Autowired @Qualifier("executorChannel") ExecutorChannel executorChannel;

  @Autowired Tracer tracer;

  @Autowired List<zipkin2.Span> spans;

  @Autowired MessagingTemplate messagingTemplate;

  Message<?> message;
  Span currentSpan;

  @Override public void handleMessage(Message<?> msg) {
    message = msg;
    currentSpan = tracer.currentSpan();
    if (message.getHeaders().containsKey("THROW_EXCEPTION")) {
      throw new RuntimeException("A terrible exception has occurred");
    }
  }

  @Before public void init() {
    directChannel.subscribe(this);
    executorChannel.subscribe(this);
  }

  @After public void close() {
    directChannel.unsubscribe(this);
    executorChannel.unsubscribe(this);
  }

  // formerly known as TraceChannelInterceptorTest.executableSpanCreation
  @Test public void propagatesNoopSpan() {
    executorChannel.send(MessageBuilder.withPayload("hi")
        .setHeader("X-B3-Sampled", "0").build());

    assertThat(message.getHeaders())
        .containsEntry("X-B3-Sampled", "0");

    assertThat(currentSpan.isNoop()).isTrue();
  }

  @Test public void messageHeadersStillMutable() {
    directChannel.send(MessageBuilder.withPayload("hi")
        .setHeader("X-B3-Sampled", "0").build());

    assertThat(MessageHeaderAccessor.getAccessor(message, MessageHeaderAccessor.class))
        .isNotNull();
  }

  @Test public void parentSpanIncluded() {
    directChannel.send(MessageBuilder.withPayload("hi")
        .setHeader("X-B3-TraceId", "0000000000000001")
        .setHeader("X-B3-SpanId", "0000000000000002").build());

    assertThat(currentSpan.isNoop()).isFalse();

    assertThat(currentSpan.context().parentId())
        .isEqualTo(2L);
  }

  @Test public void spanCreation_onSend() {
    interceptor.mode = Mode.SEND;

    directChannel.send(MessageBuilder.withPayload("hi").build());

    assertThat(message.getHeaders())
        .containsKeys("X-B3-TraceId", "X-B3-SpanId")
        .doesNotContainKeys("X-B3-ParentSpanId")
        .containsEntry("X-B3-Sampled", "1");

    // the current handler is in a span
    assertThat(currentSpan.isNoop()).isFalse();
  }

  @Test public void spanCreation_onHandle() {
    directChannel.send(MessageBuilder.withPayload("hi").build());

    // no header in injection as the span was created on handle
    assertThat(message.getHeaders())
        .doesNotContainKeys("X-B3-TraceId", "X-B3-SpanId");

    // the current handler is in a span
    assertThat(currentSpan.isNoop()).isFalse();

    assertThat(spans)
        .hasSize(1)
        .flatExtracting(zipkin2.Span::kind)
        .containsExactly(zipkin2.Span.Kind.PRODUCER);
  }

  //@Test
  //public void shouldLogClientReceivedClientSentEventWhenTheMessageIsSentAndReceived() {
  //  this.directChannel.send(MessageBuilder.withPayload("hi").build());
  //
  //  then(this.accumulator.getSpans()).hasSize(1);
  //  then(this.accumulator.getSpans().get(0).logs()).extracting("event").contains(Span.CLIENT_SEND,
  //      Span.CLIENT_RECV);
  //}
  //
  //@Test
  //public void shouldLogServerReceivedServerSentEventWhenTheMessageIsPropagatedToTheNextListener() {
  //  this.directChannel.send(MessageBuilder.withPayload("hi")
  //      .setHeader(TraceMessageHeaders.MESSAGE_SENT_FROM_CLIENT, true).build());
  //
  //  then(this.accumulator.getSpans()).hasSize(1);
  //  then(this.accumulator.getSpans().get(0).logs()).extracting("event").contains(Span.SERVER_RECV,
  //      Span.SERVER_SEND);
  //}
  //
  //@Test
  //public void headerCreation() {
  //  Span currentSpan = this.tracer.createSpan("http:testSendMessage", new AlwaysSampler());
  //  this.directChannel.send(MessageBuilder.withPayload("hi").build());
  //  this.tracer.close(currentSpan);
  //  then(this.message).isNotNull();
  //
  //  String spanId = this.message.getHeaders().get(TraceMessageHeaders.SPAN_ID_NAME, String.class);
  //  then(spanId).isNotNull();
  //
  //  String traceId = this.message.getHeaders().get(TraceMessageHeaders.TRACE_ID_NAME, String.class);
  //  then(traceId).isNotNull();
  //  then(TestSpanContextHolder.getCurrentSpan()).isNull();
  //}
  //
  //// TODO: Refactor to parametrized test together with sending messages via channel
  //@Test
  //public void headerCreationViaMessagingTemplate() {
  //  Span currentSpan = this.tracer.createSpan("http:testSendMessage", new AlwaysSampler());
  //  this.messagingTemplate.send(MessageBuilder.withPayload("hi").build());
  //
  //  this.tracer.close(currentSpan);
  //  then(this.message).isNotNull();
  //
  //  String spanId = this.message.getHeaders().get(TraceMessageHeaders.SPAN_ID_NAME, String.class);
  //  then(spanId).isNotNull();
  //
  //  String traceId = this.message.getHeaders().get(TraceMessageHeaders.TRACE_ID_NAME, String.class);
  //  then(traceId).isNotNull();
  //  then(TestSpanContextHolder.getCurrentSpan()).isNull();
  //}
  //
  //@Test
  //public void shouldCloseASpanWhenExceptionOccurred() {
  //  Span currentSpan = this.tracer.createSpan("http:testSendMessage", new AlwaysSampler());
  //  Map<String, String> errorHeaders = new HashMap<>();
  //  errorHeaders.put("THROW_EXCEPTION", "TRUE");
  //
  //  try {
  //    this.messagingTemplate.send(
  //        MessageBuilder.withPayload("hi").copyHeaders(errorHeaders).build());
  //    SleuthAssertions.fail("Exception should occur");
  //  }
  //  catch (RuntimeException e) {
  //  }
  //
  //  then(this.message).isNotNull();
  //  this.tracer.close(currentSpan);
  //  then(TestSpanContextHolder.getCurrentSpan()).isNull();
  //  then(new ListOfSpans(this.accumulator.getSpans()))
  //      .hasASpanWithTagEqualTo(Span.SPAN_ERROR_TAG_NAME,
  //          "A terrible exception has occurred");
  //}
  //
  //@Test
  //public void shouldNotTraceIgnoredChannel() {
  //  this.ignoredChannel.send(MessageBuilder.withPayload("hi").build());
  //  then(this.message).isNotNull();
  //
  //  String spanId = this.message.getHeaders().get(TraceMessageHeaders.SPAN_ID_NAME, String.class);
  //  then(spanId).isNull();
  //
  //  String traceId = this.message.getHeaders().get(TraceMessageHeaders.TRACE_ID_NAME, String.class);
  //  then(traceId).isNull();
  //
  //  then(this.accumulator.getSpans()).isEmpty();
  //  then(TestSpanContextHolder.getCurrentSpan()).isNull();
  //}
  //
  //@Test
  //public void downgrades128bitIdsByDroppingHighBits() {
  //  String hex128Bits = "463ac35c9f6413ad48485a3953bb6124";
  //  String lower64Bits = "48485a3953bb6124";
  //  this.directChannel.send(MessageBuilder.withPayload("hi")
  //      .setHeader(TraceMessageHeaders.TRACE_ID_NAME, hex128Bits)
  //      .setHeader(TraceMessageHeaders.SPAN_ID_NAME, Span.idToHex(20L)).build());
  //  then(this.message).isNotNull();
  //
  //  long traceId = Span.hexToId(this.message.getHeaders()
  //      .get(TraceMessageHeaders.TRACE_ID_NAME, String.class));
  //  then(traceId).isEqualTo(Span.hexToId(lower64Bits));
  //}
  //
  //@Test
  //public void shouldNotBreakWhenInvalidHeadersAreSent() {
  //  this.directChannel.send(MessageBuilder.withPayload("hi")
  //      .setHeader(TraceMessageHeaders.PARENT_ID_NAME, "-")
  //      .setHeader(TraceMessageHeaders.TRACE_ID_NAME, Span.idToHex(10L))
  //      .setHeader(TraceMessageHeaders.SPAN_ID_NAME, Span.idToHex(20L)).build());
  //
  //  then(this.message).isNotNull();
  //  then(this.accumulator.getSpans()).isNotEmpty();
  //  then(TestSpanContextHolder.getCurrentSpan()).isNull();
  //}
  //
  //@Test
  //public void shouldShortenTheNameWhenItsTooLarge() {
  //  this.directChannel.send(MessageBuilder.withPayload("hi")
  //      .setHeader(TraceMessageHeaders.SPAN_NAME_NAME, bigName())
  //      .setHeader(TraceMessageHeaders.TRACE_ID_NAME, Span.idToHex(10L))
  //      .setHeader(TraceMessageHeaders.SPAN_ID_NAME, Span.idToHex(20L)).build());
  //
  //  then(this.message).isNotNull();
  //
  //  then(this.accumulator.getSpans()).isNotEmpty();
  //  this.accumulator.getSpans().forEach(span1 -> then(span1.getName().length()).isLessThanOrEqualTo(50));
  //  then(TestSpanContextHolder.getCurrentSpan()).isNull();
  //}
  //
  //private String bigName() {
  //  StringBuilder sb = new StringBuilder();
  //  for (int i = 0; i < 60; i++) {
  //    sb.append("a");
  //  }
  //  return sb.toString();
  //}
  //
  //@Test
  //public void serializeMutableHeaders() throws Exception {
  //  Map<String, Object> headers = new HashMap<>();
  //  headers.put("foo", "bar");
  //  Message<?> message = new GenericMessage<>("test", headers);
  //  ChannelInterceptor immutableMessageInterceptor = new ChannelInterceptorAdapter() {
  //    @Override
  //    public Message<?> preSend(Message<?> message, MessageChannel channel) {
  //      MessageHeaderAccessor headers = MessageHeaderAccessor.getMutableAccessor(message);
  //      return new GenericMessage<Object>(message.getPayload(), headers.toMessageHeaders());
  //    }
  //  };
  //  this.directChannel.addInterceptor(immutableMessageInterceptor);
  //
  //  this.directChannel.send(message);
  //
  //  Message<?> output = (Message<?>) SerializationUtils.deserialize(SerializationUtils.serialize(this.message));
  //  then(output.getPayload()).isEqualTo("test");
  //  then(output.getHeaders().get("foo")).isEqualTo("bar");
  //  this.directChannel.removeInterceptor(immutableMessageInterceptor);
  //}
  //
  //@Test
  //public void workWithMessagingException() throws Exception {
  //  Message<?> message = new GenericMessage<>(new MessagingException(
  //      MessageBuilder.withPayload("hi")
  //          .setHeader(TraceMessageHeaders.TRACE_ID_NAME, Span.idToHex(10L))
  //          .setHeader(TraceMessageHeaders.SPAN_ID_NAME, Span.idToHex(20L)).build()
  //  ));
  //
  //  this.directChannel.send(message);
  //
  //  String spanId = this.message.getHeaders().get(TraceMessageHeaders.SPAN_ID_NAME, String.class);
  //  then(message.getPayload()).isEqualTo(this.message.getPayload());
  //  then(spanId).isNotNull();
  //  long traceId = Span
  //      .hexToId(this.message.getHeaders().get(TraceMessageHeaders.TRACE_ID_NAME, String.class));
  //  then(traceId).isEqualTo(10L);
  //  then(spanId).isNotEqualTo(20L);
  //  then(this.accumulator.getSpans()).hasSize(1);
  //}
  //
  //@Test
  //public void errorMessageHeadersRetained() {
  //  QueueChannel deadReplyChannel = new QueueChannel();
  //  QueueChannel errorsReplyChannel = new QueueChannel();
  //  Map<String, Object> errorChannelHeaders = new HashMap<>();
  //  errorChannelHeaders.put(MessageHeaders.REPLY_CHANNEL, errorsReplyChannel);
  //  errorChannelHeaders.put(MessageHeaders.ERROR_CHANNEL, errorsReplyChannel);
  //
  //  this.directChannel.send(new ErrorMessage(
  //      new MessagingException(MessageBuilder.withPayload("hi")
  //          .setHeader(TraceMessageHeaders.TRACE_ID_NAME, Span.idToHex(10L))
  //          .setHeader(TraceMessageHeaders.SPAN_ID_NAME, Span.idToHex(20L))
  //          .setReplyChannel(deadReplyChannel)
  //          .setErrorChannel(deadReplyChannel)
  //          .build()	),
  //      errorChannelHeaders));
  //  then(this.message).isNotNull();
  //
  //  String spanId = this.message.getHeaders().get(TraceMessageHeaders.SPAN_ID_NAME, String.class);
  //  then(spanId).isNotNull();
  //  long traceId = Span
  //      .hexToId(this.message.getHeaders().get(TraceMessageHeaders.TRACE_ID_NAME, String.class));
  //  then(traceId).isEqualTo(10L);
  //  then(spanId).isNotEqualTo(20L);
  //  then(this.accumulator.getSpans()).hasSize(1);
  //  then(this.message.getHeaders().getReplyChannel()).isSameAs(errorsReplyChannel);
  //  then(this.message.getHeaders().getErrorChannel()).isSameAs(errorsReplyChannel);
  //}

  @Configuration
  @EnableAutoConfiguration
  static class App {

    @Bean List<zipkin2.Span> spans() {
      return new ArrayList<>();
    }

    @Bean Tracing tracing() {
      return Tracing.newBuilder()
          .currentTraceContext(new StrictCurrentTraceContext())
          .spanReporter(spans()::add)
          .build();
    }

    @Bean Tracer tracer() {
      return tracing().tracer();
    }

    @Bean Executor executor() {
      return Executors.newSingleThreadExecutor();
    }

    @Bean ExecutorChannel executorChannel() {
      return new ExecutorChannel(executor());
    }

    @Bean DirectChannel directChannel() {
      return new DirectChannel();
    }

    @Bean public MessagingTemplate messagingTemplate() {
      return new MessagingTemplate(directChannel());
    }

    @Bean @GlobalChannelInterceptor
    public ChannelInterceptor tracingChannelInterceptor(Tracing tracing) {
      return new SelectableChannelInterceptor(TracingChannelInterceptor.create(tracing));
    }
  }
}