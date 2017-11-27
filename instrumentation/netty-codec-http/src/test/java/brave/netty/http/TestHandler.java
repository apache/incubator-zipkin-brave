package brave.netty.http;

import brave.http.HttpTracing;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import java.io.IOException;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;
import static io.netty.util.CharsetUtil.UTF_8;

class TestHandler extends ChannelInboundHandlerAdapter {
  final HttpTracing httpTracing;
  HttpRequest req;

  TestHandler(HttpTracing httpTracing) {
    this.httpTracing = httpTracing;
  }

  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
    if (msg instanceof HttpRequest) {
      req = (HttpRequest) msg;
      String uri = req.uri();
      String content = null;
      HttpResponseStatus status = OK;
      if (uri.startsWith("/foo")) {
        content = "bar";
      } else if (uri.startsWith("/child")) {
        httpTracing.tracing().tracer().nextSpan().name("child").start().finish();
        content = "happy";
      } else if (uri.startsWith("/exception")) {
        throw new IOException("exception");
      } else if (uri.startsWith("/async")) {
        content = "async";
      } else if (uri.startsWith("/badrequest")) {
        status = BAD_REQUEST;
      } else {
        status = NOT_FOUND;
      }

      writeResponse(ctx, status, content);
    }
  }

  @Override public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

  void writeResponse(ChannelHandlerContext ctx, HttpResponseStatus responseStatus, String content) {
    if (HttpUtil.is100ContinueExpected(req)) {
      ctx.write(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE));
    }
    boolean keepAlive = HttpUtil.isKeepAlive(req);
    FullHttpResponse response;
    if (content != null) {
      response = new DefaultFullHttpResponse(HTTP_1_1, OK, Unpooled.copiedBuffer(content, UTF_8));
      response.headers().set(CONTENT_TYPE, "text/plain");
      response.headers().set(CONTENT_LENGTH, response.content().readableBytes());
    } else {
      response = new DefaultFullHttpResponse(HTTP_1_1, OK);
    }
    response.setStatus(responseStatus);

    if (!keepAlive) {
      ctx.write(response).addListener(ChannelFutureListener.CLOSE);
    } else {
      response.headers().set(CONNECTION, HttpHeaderValues.KEEP_ALIVE);
      ctx.write(response);
    }
  }

  @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    ctx.fireExceptionCaught(cause);
    writeResponse(ctx, INTERNAL_SERVER_ERROR, null);
  }
}
