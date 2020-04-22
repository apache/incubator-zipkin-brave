/*
 * Copyright 2013-2020 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package brave.dubbo;

import brave.Span;
import brave.internal.Nullable;
import brave.internal.Platform;
import java.net.InetSocketAddress;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.support.RpcUtils;

final class DubboParser {
  /**
   * Returns the method name of the invocation or the first string arg of an "$invoke" or
   * "$invokeAsync" method.
   *
   * <p>Like {@link RpcUtils#getMethodName(Invocation)}, except without re-reading fields or
   * returning an unhelpful "$invoke" method name.
   */
  @Nullable static String method(Invocation invocation) {
    String methodName = invocation.getMethodName();
    if ("$invoke".equals(methodName) || "$invokeAsync".equals(methodName)) {
      Object[] arguments = invocation.getArguments();
      if (arguments != null && arguments.length > 0 && arguments[0] instanceof String) {
        methodName = (String) arguments[0];
      } else {
        methodName = null;
      }
    }
    return methodName != null && !methodName.isEmpty() ? methodName : null;
  }

  /**
   * Returns the {@link URL#getServiceInterface() service interface} of the invocation.
   *
   * <p>This was chosen as the {@link URL#getServiceName() service name} is deprecated for it.
   */
  @Nullable static String service(Invocation invocation) {
    Invoker<?> invoker = invocation.getInvoker();
    if (invoker == null) return null;
    URL url = invoker.getUrl();
    if (url == null) return null;
    String service = url.getServiceInterface();
    return service != null && !service.isEmpty() ? service : null;
  }

  static boolean parseRemoteIpAndPort(Span span) {
    RpcContext rpcContext = RpcContext.getContext();
    InetSocketAddress remoteAddress = rpcContext.getRemoteAddress();
    if (remoteAddress == null) return false;
    return span.remoteIpAndPort(
      Platform.get().getHostString(remoteAddress),
      remoteAddress.getPort()
    );
  }

  /**
   * On occasion, (roughly once a year) Dubbo adds more error code numbers. When this occurs, do
   * not use the symbol name, in the switch statement, as it will affect the minimum version.
   */
  @Nullable static String errorCode(Throwable error) {
    if (error instanceof RpcException) {
      int code = ((RpcException) error).getCode();
      switch (code) { // requires maintenance if constants are updated
        case RpcException.UNKNOWN_EXCEPTION:
          return "UNKNOWN_EXCEPTION";
        case RpcException.NETWORK_EXCEPTION:
          return "NETWORK_EXCEPTION";
        case RpcException.TIMEOUT_EXCEPTION:
          return "TIMEOUT_EXCEPTION";
        case RpcException.BIZ_EXCEPTION:
          return "BIZ_EXCEPTION";
        case RpcException.FORBIDDEN_EXCEPTION:
          return "FORBIDDEN_EXCEPTION";
        case RpcException.SERIALIZATION_EXCEPTION:
          return "SERIALIZATION_EXCEPTION";
        case RpcException.NO_INVOKER_AVAILABLE_AFTER_FILTER:
          return "NO_INVOKER_AVAILABLE_AFTER_FILTER";
        case 7: // RpcException.LIMIT_EXCEEDED_EXCEPTION Added in 2.7.3
          return "LIMIT_EXCEEDED_EXCEPTION";
        default:
          return String.valueOf(code);
      }
    }
    return null;
  }
}
