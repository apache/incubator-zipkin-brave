package brave.internal.recorder;

import brave.Span;
import brave.Tracing;
import brave.internal.Platform;
import brave.propagation.TraceContext;
import org.junit.After;
import org.junit.Test;
import zipkin.internal.V2SpanConverter;
import zipkin.internal.v2.Annotation;
import zipkin.internal.v2.Endpoint;

import static brave.Span.Kind.CLIENT;
import static brave.Span.Kind.SERVER;
import static org.assertj.core.api.Assertions.assertThat;

public class MutableSpanTest {
  Endpoint localEndpoint = Platform.get().localEndpoint();
  TraceContext context = Tracing.newBuilder().build().tracer().newTrace().context();

  @After public void close() {
    Tracing.current().close();
  }

  // zipkin needs one annotation or binary annotation so that the local endpoint can be read
  @Test public void addsLocalEndpoint() {
    MutableSpan span = newSpan();

    span.start(1L);
    span.finish(2L);

    assertThat(span.toSpan().localEndpoint())
        .isEqualTo(localEndpoint);
  }

  @Test public void minimumDurationIsOne() {
    MutableSpan span = newSpan();

    span.start(1L).finish(1L);

    assertThat(span.toSpan().duration()).isEqualTo(1L);
  }

  @Test public void addsAnnotations() {
    MutableSpan span = newSpan();

    span.start(1L);
    span.annotate(2L, "foo");
    span.finish(2L);

    assertThat(span.toSpan().annotations())
        .containsOnly(Annotation.create(2L, "foo"));
  }

  @Test public void finished_client() {
    finish(Span.Kind.CLIENT, zipkin.internal.v2.Span.Kind.CLIENT);
  }

  @Test public void finished_server() {
    finish(Span.Kind.SERVER, zipkin.internal.v2.Span.Kind.SERVER);
  }

  @Test public void finished_producer() {
    finish(Span.Kind.PRODUCER, zipkin.internal.v2.Span.Kind.PRODUCER);
  }

  @Test public void finished_consumer() {
    finish(Span.Kind.CONSUMER, zipkin.internal.v2.Span.Kind.CONSUMER);
  }

  private void finish(Span.Kind braveKind, zipkin.internal.v2.Span.Kind span2Kind) {
    MutableSpan span = newSpan();
    span.kind(braveKind);
    span.start(1L);
    span.finish(2L);

    zipkin.internal.v2.Span span2 = span.toSpan();
    assertThat(span2.annotations()).isEmpty();
    assertThat(span2.timestamp()).isEqualTo(1L);
    assertThat(span2.duration()).isEqualTo(1L);
    assertThat(span2.kind()).isEqualTo(span2Kind);
  }

  @Test public void flushed_client() {
    flush(Span.Kind.CLIENT, zipkin.internal.v2.Span.Kind.CLIENT);
  }

  @Test public void flushed_server() {
    flush(Span.Kind.SERVER, zipkin.internal.v2.Span.Kind.SERVER);
  }

  @Test public void flushed_producer() {
    flush(Span.Kind.PRODUCER, zipkin.internal.v2.Span.Kind.PRODUCER);
  }

  @Test public void flushed_consumer() {
    flush(Span.Kind.CONSUMER, zipkin.internal.v2.Span.Kind.CONSUMER);
  }

  private void flush(Span.Kind braveKind, zipkin.internal.v2.Span.Kind span2Kind) {
    MutableSpan span = newSpan();
    span.kind(braveKind);
    span.start(1L);
    span.finish(null);

    zipkin.internal.v2.Span span2 = span.toSpan();
    assertThat(span2.annotations()).isEmpty();
    assertThat(span2.timestamp()).isEqualTo(1L);
    assertThat(span2.duration()).isNull();
    assertThat(span2.kind()).isEqualTo(span2Kind);
  }

  @Test public void remoteEndpoint() {
    MutableSpan span = newSpan();

    Endpoint endpoint = Endpoint.newBuilder().serviceName("server").ip("127.0.0.1").build();
    span.kind(CLIENT);
    span.remoteEndpoint(endpoint);
    span.start(1L);
    span.finish(2L);

    assertThat(span.toSpan().remoteEndpoint()).isEqualTo(endpoint);
  }

  // This prevents the server timestamp from overwriting the client one
  @Test public void doesntReportServerTimestampOnSharedSpans() {
    MutableSpan span = new MutableSpan(context.toBuilder().shared(true).build(), localEndpoint);

    span.start(1L);
    span.kind(SERVER);
    span.finish(2L);

    assertThat(V2SpanConverter.toSpan(span.toSpan())).extracting(s -> s.timestamp, s -> s.duration)
        .allSatisfy(u -> assertThat(u).isNull());
  }

  @Test public void flushUnstartedNeitherSetsTimestampNorDuration() {
    zipkin.internal.v2.Span flushed = newSpan().finish(null).toSpan();
    assertThat(flushed).extracting(s -> s.timestamp(), s -> s.duration())
        .allSatisfy(u -> assertThat(u).isNull());
  }

  /** We can't compute duration unless we started the span in the same tracer. */
  @Test public void finishUnstartedIsSameAsFlush() {
    assertThat(newSpan().finish(2L).toSpan())
        .isEqualTo(newSpan().finish(null).toSpan());
  }

  MutableSpan newSpan() {
    return new MutableSpan(context, localEndpoint);
  }
}
