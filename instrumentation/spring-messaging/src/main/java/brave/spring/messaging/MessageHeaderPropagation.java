package brave.spring.messaging;

import brave.propagation.Propagation;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.messaging.support.NativeMessageHeaderAccessor;
import org.springframework.util.LinkedMultiValueMap;

import static org.springframework.messaging.support.NativeMessageHeaderAccessor.NATIVE_HEADERS;

/**
 * This always sets native headers in defence of STOMP issues discussed <a href="https://github.com/spring-cloud/spring-cloud-sleuth/issues/716#issuecomment-337523705">here</a>
 */
enum MessageHeaderPropagation implements
    Propagation.Setter<MessageHeaderAccessor, String>,
    Propagation.Getter<MessageHeaderAccessor, String> {
  INSTANCE;

  @Override public void put(MessageHeaderAccessor accessor, String key, String value) {
    accessor.setHeader(key, value);
    if (accessor instanceof NativeMessageHeaderAccessor) {
      NativeMessageHeaderAccessor nativeAccessor = (NativeMessageHeaderAccessor) accessor;
      nativeAccessor.setNativeHeader(key, value);
    } else {
      Map<String, List<String>> nativeHeaders = (Map) accessor.getHeader(NATIVE_HEADERS);
      if (nativeHeaders == null) {
        accessor.setHeader(NATIVE_HEADERS, nativeHeaders = new LinkedMultiValueMap<>());
      }
      nativeHeaders.put(key, Collections.singletonList(value));
    }
  }

  @Override public String get(MessageHeaderAccessor accessor, String key) {
    if (accessor instanceof NativeMessageHeaderAccessor) {
      NativeMessageHeaderAccessor nativeAccessor = (NativeMessageHeaderAccessor) accessor;
      String result = nativeAccessor.getFirstNativeHeader(key);
      if (result != null) return result;
    } else {
      Map<String, List<String>> nativeHeaders = (Map) accessor.getHeader(NATIVE_HEADERS);
      if (nativeHeaders != null) {
        List<String> result = nativeHeaders.get(key);
        if (result != null && !result.isEmpty()) return result.get(0);
      }
    }
    Object result = accessor.getHeader(key);
    return result != null ? result.toString() : null;
  }

  static void removeAnyTraceHeaders(MessageHeaderAccessor accessor, List<String> keysToRemove) {
    for (String keyToRemove : keysToRemove) {
      accessor.removeHeader(keyToRemove);
      if (accessor instanceof NativeMessageHeaderAccessor) {
        NativeMessageHeaderAccessor nativeAccessor = (NativeMessageHeaderAccessor) accessor;
        nativeAccessor.removeNativeHeader(keyToRemove);
      } else {
        Map<String, List<String>> nativeHeaders = (Map) accessor.getHeader(NATIVE_HEADERS);
        if (nativeHeaders == null) continue;
        nativeHeaders.remove(keyToRemove);
      }
    }
  }

  @Override public String toString() {
    return "MessageHeaderPropagation{}";
  }
}
