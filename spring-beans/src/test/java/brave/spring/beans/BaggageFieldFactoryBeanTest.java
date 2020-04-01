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
package brave.spring.beans;

import brave.propagation.BaggageField;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BaggageFieldFactoryBeanTest {
  XmlBeans context;

  @After public void close() {
    if (context != null) context.close();
  }

  @Test public void name() {
    context = new XmlBeans(""
      + "<bean id=\"userId\" class=\"brave.spring.beans.BaggageFieldFactoryBean\">\n"
      + "  <property name=\"name\" value=\"userId\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("userId", BaggageField.class))
      .isEqualTo(BaggageField.create("userId"));
  }

  @Test public void local() {
    context = new XmlBeans(""
      + "<bean id=\"userId\" class=\"brave.spring.beans.BaggageFieldFactoryBean\">\n"
      + "  <property name=\"name\" value=\"userId\"/>\n"
      + "  <property name=\"remoteNames\">\n"
      + "    <list>\n"
      + "    </list>\n"
      + "  </property>"
      + "</bean>"
    );

    assertThat(context.getBean("userId", BaggageField.class))
      .isEqualToComparingFieldByField(BaggageField.newBuilder("userId").clearRemoteNames().build());
  }

  @Test public void flushOnUpdate() {
    context = new XmlBeans(""
      + "<bean id=\"userId\" class=\"brave.spring.beans.BaggageFieldFactoryBean\">\n"
      + "  <property name=\"name\" value=\"userId\"/>\n"
      + "  <property name=\"flushOnUpdate\" value=\"true\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("userId", BaggageField.class))
      .isEqualToComparingFieldByField(BaggageField.newBuilder("userId").flushOnUpdate().build());
  }

  @Test public void remoteNames() {
    context = new XmlBeans(""
      + "<bean id=\"userId\" class=\"brave.spring.beans.BaggageFieldFactoryBean\">\n"
      + "  <property name=\"name\" value=\"userId\"/>\n"
      + "  <property name=\"remoteNames\">\n"
      + "    <list>\n"
      + "      <value>baggage_user_id</value>\n"
      + "      <value>baggage-user-id</value>\n"
      + "    </list>\n"
      + "  </property>"
      + "</bean>"
    );

    assertThat(context.getBean("userId", BaggageField.class))
      .isEqualToComparingFieldByField(BaggageField.newBuilder("userId").clearRemoteNames()
        .addRemoteName("baggage_user_id")
        .addRemoteName("baggage-user-id").build()
      );
  }
}