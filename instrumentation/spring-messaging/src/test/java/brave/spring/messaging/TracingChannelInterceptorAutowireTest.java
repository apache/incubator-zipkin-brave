package brave.spring.messaging;

import brave.Tracing;
import org.junit.After;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.support.ChannelInterceptor;

public class TracingChannelInterceptorAutowireTest {

  @Configuration static class TracingConfiguration {
    @Bean Tracing tracing() {
      return Tracing.newBuilder().build();
    }
  }

  @Test public void autowiredWithBeanConfig() {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    ctx.register(TracingConfiguration.class);
    ctx.register(TracingChannelInterceptor.class);
    ctx.refresh();

    ctx.getBean(ChannelInterceptor.class);
  }

  @After public void close() {
    Tracing.current().close();
  }
}
