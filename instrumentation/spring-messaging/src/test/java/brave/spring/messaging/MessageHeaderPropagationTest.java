package brave.spring.messaging;

import brave.propagation.Propagation;
import brave.test.propagation.PropagationSetterTest;
import java.util.Collections;
import org.springframework.messaging.support.MessageHeaderAccessor;

public class MessageHeaderPropagationTest
    extends PropagationSetterTest<MessageHeaderAccessor, String> {
  MessageHeaderAccessor carrier = new MessageHeaderAccessor();

  @Override public Propagation.KeyFactory<String> keyFactory() {
    return Propagation.KeyFactory.STRING;
  }

  @Override protected MessageHeaderAccessor carrier() {
    return carrier;
  }

  @Override protected Propagation.Setter<MessageHeaderAccessor, String> setter() {
    return MessageHeaderPropagation.INSTANCE;
  }

  @Override protected Iterable<String> read(MessageHeaderAccessor carrier, String key) {
    Object result = carrier.getHeader(key);
    return result != null ? Collections.singleton(result.toString()) : Collections.emptyList();
  }
}
