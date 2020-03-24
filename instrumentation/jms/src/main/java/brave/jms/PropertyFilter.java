/*
 * Copyright 2013-2019 The OpenZipkin Authors
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
package brave.jms;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;

import static brave.internal.Throwables.propagateIfFatal;
import static brave.jms.JmsTracing.log;
import static java.util.Collections.list;

// Similar to https://github.com/apache/camel/blob/b9a3117f19dd19abd2ea8b789c42c3e86fe4c488/components/camel-jms/src/main/java/org/apache/camel/component/jms/JmsMessageHelper.java
final class PropertyFilter {
  /**
   * This implies copying properties because the JMS spec says you can't write properties upon
   * receipt until {@link Message#clearProperties()} has been called.
   *
   * <p> See https://docs.oracle.com/javaee/6/api/javax/jms/Message.html
   */
  static void filterProperties(Message message, Set<String> namesToClear) {
    ArrayList<Object> retainedProperties = messagePropertiesBuffer();
    try {
      filterProperties(message, namesToClear, retainedProperties);
    } finally {
      retainedProperties.clear(); // ensure no object references are held due to any exception
    }
  }

  static void filterProperties(Message message, Set<String> namesToClear, List<Object> out) {
    List<String> names;
    try {
      names = list(message.getPropertyNames());
    } catch (Throwable t) {
      propagateIfFatal(t);
      log(t, "error getting property names from {0}", message, null);
      return;
    }
    
    for (String name: names) {
      if (!namesToClear.contains(name)) {
        Object value;
        try {
          value = message.getObjectProperty(name);
        } catch (Throwable t) {
          propagateIfFatal(t);
          log(t, "error getting property {0} from message {1}", name, message);
          return;
        }
        if (value != null) {
          out.add(name);
          out.add(value);
        }
      }
    }

    // redo the properties to keep
    try {
      message.clearProperties();
    } catch (Throwable t) {
      propagateIfFatal(t);
      log(t, "error clearing properties of {0}", message, null);
      return;
    }

    // workaround for ActiveMQBytesMessage
    if (message instanceof BytesMessage) { // BytesMessage requires clearing the body before reset properties otherwise it fails with MessageNotWriteableException
      resetBytesMessageProperties(message, out);
    } else {
      resetProperties(message, out);
    }
  }

  private static void resetBytesMessageProperties( Message message, List<Object> out ) {
    try {
      BytesMessage bytesMessage = (BytesMessage) message;
      byte[] body = new byte[(int) bytesMessage.getBodyLength()];
      bytesMessage.reset();
      bytesMessage.readBytes(body);
      // setObjectProperty on BytesMessage requires clearing the body otherwise it fails with MessageNotWriteableException
      message.clearBody();
      resetProperties(message, out);
      bytesMessage.writeBytes(body);
    } catch (JMSException e) {
      propagateIfFatal(e);
      log(e, "unable to reset BytesMessage body {0}", message, e);
    }
  }

  private static void resetProperties(Message message, List<Object> out) {
    for (int i = 0, length = out.size(); i < length; i += 2) {
      String name = out.get(i).toString();
      try {
        message.setObjectProperty(name, out.get(i + 1));
      } catch (Throwable t) {
        propagateIfFatal(t);
        log(t, "error setting property {0} on message {1}", name, message);
        // continue on error when re-setting properties as it is better than not.
      }
    }
  }

  static final ThreadLocal<ArrayList<Object>> MESSAGE_PROPERTIES_BUFFER = new ThreadLocal<>();

  /** Also use pair indexing for temporary message properties: (name, value). */
  static ArrayList<Object> messagePropertiesBuffer() {
    ArrayList<Object> messagePropertiesBuffer = MESSAGE_PROPERTIES_BUFFER.get();
    if (messagePropertiesBuffer == null) {
      messagePropertiesBuffer = new ArrayList<>();
      MESSAGE_PROPERTIES_BUFFER.set(messagePropertiesBuffer);
    }
    return messagePropertiesBuffer;
  }
}
