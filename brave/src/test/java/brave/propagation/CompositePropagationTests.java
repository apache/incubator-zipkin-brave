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
package brave.propagation;

import org.junit.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class CompositePropagationTests {
  static long TRACE_ID = 1234;
  static long TRACE_ID2 = 1235;
  static long SPAN_ID = 5678;
  static Propagation.Setter<Map<String, String>, String> SETTER = Map::put;
  static Propagation.Getter<Map<String, String>, String> GETTER = Map::get;
  static TraceContext TRACE_CONTEXT = TraceContext.newBuilder()
          .traceId(TRACE_ID)
          .spanId(SPAN_ID)
          .build();
  static Map<String, String> FIELDS1 = new HashMap<>();
  static {
    FIELDS1.put("a", "1");
    FIELDS1.put("b", "2");
  }
  static Map<String, String> FIELDS2 = new HashMap<>();
  static {
    FIELDS2.put("a", "3");
    FIELDS2.put("c", "4");
  }

  @Test public void testFactorySupportsJoin() {
    Propagation.Factory factory = CompositePropagation.newFactoryBuilder()
      .addPropagationFactory(new MockFactory() {
        @Override
        public boolean supportsJoin() {
          return false;
        }
      })
      .addPropagationFactory(new MockFactory() {
        @Override
        public boolean supportsJoin() {
          return true;
        }
      })
      .build();
    assertThat(factory.supportsJoin()).isFalse(); // should be logical and
  }

  @Test public void testFactoryRequires128BitTraceId() {
    Propagation.Factory factory = CompositePropagation.newFactoryBuilder()
      .addPropagationFactory(new MockFactory() {
        @Override
        public boolean requires128BitTraceId() {
          return false;
        }
      })
      .addPropagationFactory(new MockFactory() {
        @Override
        public boolean requires128BitTraceId() {
          return true;
        }
      })
      .build();
    assertThat(factory.requires128BitTraceId()).isTrue(); // should be logical or
  }

  @Test public void testKeys() {
    Propagation<String> propagation = CompositePropagation.newFactoryBuilder()
      .addPropagationFactory(MapInjectingPropagation.newFactory(FIELDS1))
      .addPropagationFactory(MapInjectingPropagation.newFactory(FIELDS2))
      .build()
      .get();
    assertThat(propagation.keys()).containsExactly("a", "b", "c");
  }

  @Test public void testInjectAll() {
    Propagation<String> propagation = CompositePropagation.newFactoryBuilder()
      .addPropagationFactory(MapInjectingPropagation.newFactory(FIELDS1))
      .addPropagationFactory(MapInjectingPropagation.newFactory(FIELDS2))
      .build()
      .get();
    Map<String, String> carrier = new HashMap<>();
    propagation.injector(SETTER).inject(TRACE_CONTEXT, carrier);
    assertThat(carrier).contains(
      entry("a", "3"),
      entry("b", "2"),
      entry("c", "4")
    );
  }

  @Test public void testInjectFirst() {
    Propagation<String> propagation = CompositePropagation.newFactoryBuilder()
      .addPropagationFactory(MapInjectingPropagation.newFactory(FIELDS1))
      .addPropagationFactory(MapInjectingPropagation.newFactory(FIELDS2))
      .injectAll(false)
      .build()
      .get();
    Map<String, String> carrier = new HashMap<>();
    propagation.injector(SETTER).inject(TRACE_CONTEXT, carrier);
    assertThat(carrier).contains(
      entry("a", "1"),
      entry("b", "2")
    );
  }

  @Test public void testExtract() {
    TraceContext.Extractor<Map<String, String>> extractor1 = CompositePropagation.newFactoryBuilder()
      .addPropagationFactory(FieldExtractingPropagation.newFactory("a"))
      .addPropagationFactory(FieldExtractingPropagation.newFactory("b"))
      .build()
      .get()
      .extractor(GETTER);
    TraceContext.Extractor<Map<String, String>> extractor2 = CompositePropagation.newFactoryBuilder()
      .addPropagationFactory(FieldExtractingPropagation.newFactory("b"))
      .addPropagationFactory(FieldExtractingPropagation.newFactory("a"))
      .build()
      .get()
      .extractor(GETTER);

    Map<String, String> carrier = new HashMap<>();
    carrier.put("a", String.valueOf(TRACE_ID));
    carrier.put("b", String.valueOf(TRACE_ID2));

    assertThat(extractor1.extract(carrier)).isEqualTo(
      TraceContextOrSamplingFlags.create(TraceContext.newBuilder()
        .traceId(TRACE_ID)
        .spanId(TRACE_ID)
        .build())
    );

    assertThat(extractor2.extract(carrier)).isEqualTo(
      TraceContextOrSamplingFlags.create(TraceContext.newBuilder()
        .traceId(TRACE_ID2)
        .spanId(TRACE_ID2)
        .build())
    );
  }
}

class MapInjectingPropagation implements Propagation<String> {
  final Map<String, String> fields;

  static Factory newFactory(Map<String, String> fields) {
    return new Factory() {
      @Deprecated
      @Override
      public <K> Propagation<K> create(KeyFactory<K> keyFactory) {
        // noinspection unchecked
        return (Propagation<K>) get();
      }

      @Override
      public Propagation<String> get() {
        return new MapInjectingPropagation(fields);
      }
    };
  }

  MapInjectingPropagation(Map<String, String> fields) {
    this.fields = fields;
  }

  @Override public List<String> keys() {
    return new ArrayList<>(fields.keySet());
  }

  @Override public <C> TraceContext.Injector<C> injector(Setter<C, String> setter) {
    return (traceContext, carrier) -> fields.forEach((key, value) -> setter.put(carrier, key, value));
  }

  @Override public <C> TraceContext.Extractor<C> extractor(Getter<C, String> getter) {
    throw new UnsupportedOperationException();
  }
}

class FieldExtractingPropagation implements Propagation<String> {
  final String key;

  static Factory newFactory(String key) {
    return new Factory() {
      @Deprecated
      @Override
      public <K> Propagation<K> create(KeyFactory<K> keyFactory) {
        // noinspection unchecked
        return (Propagation<K>) get();
      }

      @Override
      public Propagation<String> get() {
        return new FieldExtractingPropagation(key);
      }
    };
  }

  FieldExtractingPropagation(String key) {
    this.key = key;
  }

  @Override public List<String> keys() {
    return Collections.singletonList(key);
  }

  @Override public <C> TraceContext.Injector<C> injector(Setter<C, String> setter) {
    throw new UnsupportedOperationException();
  }

  @Override public <C> TraceContext.Extractor<C> extractor(Getter<C, String> getter) {
    return carrier -> {
      long value = Long.parseLong(getter.get(carrier, key));
      return TraceContextOrSamplingFlags.create(TraceContext.newBuilder()
              .traceId(value)
              .spanId(value)
              .build());
    };
  }
}

class MockFactory extends Propagation.Factory {
  @Deprecated
  @Override
  public <K> Propagation<K> create(Propagation.KeyFactory<K> keyFactory) {
    return null;
  }

  @Override
  public Propagation<String> get() {
    return null;
  }
}
