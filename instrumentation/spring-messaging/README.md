# brave-instrumentation-spring-messaging
This module contains a tracing interceptor for [Spring Messaging](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-messaging.html).
`TracingChannelInterceptor` completes a producer span per message, propagating it via headers. When
consuming, a child span is created from trace in headers if present.

## Configuration

Tracing always needs a bean of type `Tracing` configured. Make sure
it is in place before proceeding. Here's an example in [XML](../../spring-beans/README.md).

Then, wire `TracingChannelInterceptor` and add it via `InterceptableChannel.addInterceptor`, or
something that does the same thing like below:

```java
@Bean
@GlobalChannelInterceptor // handy to apply to all channels if you depend on spring-integration-core
public ChannelInterceptor tracingChannelInterceptor(Tracing tracing) {
  return TracingChannelInterceptor.create(tracing);
}
```