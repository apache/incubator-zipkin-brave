package com.github.kristofa.brave.jersey2;

import com.github.kristofa.brave.*;
import com.github.kristofa.brave.http.SpanNameProvider;
import com.github.kristofa.brave.jaxrs2.BraveClientRequestFilter;
import com.github.kristofa.brave.jaxrs2.BraveClientResponseFilter;

import zipkin.Span;

import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ITBraveJersey2 extends JerseyTest {

    private SpanNameProvider spanNameProvider;
    private ClientRequestInterceptor clientRequestInterceptor;
    private ClientResponseInterceptor clientResponseInterceptor;

    @Override
    protected Application configure() {
        ApplicationContext context = new AnnotationConfigApplicationContext(JerseyTestSpringConfig.class);
        spanNameProvider = context.getBean(SpanNameProvider.class);
        clientRequestInterceptor = context.getBean(ClientRequestInterceptor.class);
        clientResponseInterceptor = context.getBean(ClientResponseInterceptor.class);
        return new JerseyTestConfig().property("contextConfig", context);
    }

    @Test
    public void testBraveJersey2() {
        WebTarget target = target("/brave-jersey2/test");
        target.register(new BraveClientRequestFilter(spanNameProvider, clientRequestInterceptor));
        target.register(new BraveClientResponseFilter(clientResponseInterceptor));

        final Response response = target.request().get();
        assertEquals(200, response.getStatus());

        final List<Span> collectedSpans = ReporterForTesting.getInstance().getCollectedSpans();
        assertEquals(2, collectedSpans.size());
        final Span clientSpan = collectedSpans.get(0);
        final Span serverSpan = collectedSpans.get(1);

        assertEquals("Expected trace id's to be equal", clientSpan.traceId, serverSpan.traceId);
        assertEquals("Expected span id's to be equal", clientSpan.id, serverSpan.id);
        assertEquals("Expected parent span id's to be equal", clientSpan.parentId, serverSpan.parentId);
        assertEquals("Span names of client and server should be equal.", clientSpan.name, serverSpan.name);
        assertEquals("Expect 2 annotations.", 2, clientSpan.annotations.size());
        assertEquals("Expect 2 annotations.", 2, serverSpan.annotations.size());
        assertEquals("service name of end points for both client and server annotations should be equal.",
            clientSpan.annotations.get(0).endpoint.serviceName,
            serverSpan.annotations.get(0).endpoint.serviceName
        );
    }
}
