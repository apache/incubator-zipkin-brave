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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Allows multiple {@link Propagation}s to be used, which is useful when transitioning from a propagation format to
 * another.
 *
 * Upon injection, depending on the {@code injectAll} setting, only the first or all {@link Propagation}s are used,
 * while at extraction time {@link Propagation}s will be tried one by one in the specified order until one is
 * successful, at which point the remaining {@link Propagation}s will not be called. If all {@link Propagation}s fail
 * to extract, {@link TraceContextOrSamplingFlags#EMPTY} is returned.
 */
public class CompositePropagation<K> implements Propagation<K> {
  public static FactoryBuilder newFactoryBuilder() {
    return new FactoryBuilder();
  }

  /**
   * Defaults to {@code injectAll = true}.
   */
  public static final class FactoryBuilder {
    boolean injectAll = true;
    final List<Propagation.Factory> propagationFactories = new ArrayList<>();

    public FactoryBuilder addPropagationFactory(Propagation.Factory propagationFactory) {
      if (propagationFactory == null) throw new NullPointerException("propagationFactory == null");
      propagationFactories.add(propagationFactory);
      return this;
    }

    public FactoryBuilder addAllPropagationFactories(Collection<Propagation.Factory> propagationFactories) {
      if (propagationFactories == null) throw new NullPointerException("propagationFactories == null");
      this.propagationFactories.addAll(propagationFactories);
      return this;
    }

    public FactoryBuilder clearPropagationFactories() {
      propagationFactories.clear();
      return this;
    }

    public FactoryBuilder injectAll(boolean injectAll) {
      this.injectAll = injectAll;
      return this;
    }

    public Propagation.Factory build() {
      return new Factory(this);
    }
  }

  final List<Propagation<K>> propagations;
  final List<K> keys;
  final boolean injectAll;

  CompositePropagation(List<Propagation<K>> propagations, boolean injectAll) {
    this.propagations = propagations;
    this.injectAll = injectAll;
    Set<K> keySet = new LinkedHashSet<>();
    for (Propagation<K> propagation : propagations) {
      keySet.addAll(propagation.keys());
    }
    keys = new ArrayList<>(keySet);
  }

  @Override public List<K> keys() {
    return keys;
  }

  @Override public <C> TraceContext.Injector<C> injector(Setter<C, K> setter) {
    return new TraceContext.Injector<C>() {
      @Override
      public void inject(TraceContext traceContext, C carrier) {
        for (Propagation<K> propagation : propagations) {
          propagation.injector(setter).inject(traceContext, carrier);
          if (!injectAll) {
            break;
          }
        }
      }
    };
  }

  @Override public <C> TraceContext.Extractor<C> extractor(Getter<C, K> getter) {
    return new TraceContext.Extractor<C>() {
      @Override
      public TraceContextOrSamplingFlags extract(C carrier) {
        for (Propagation<K> propagation : propagations) {
          TraceContextOrSamplingFlags result = propagation.extractor(getter).extract(carrier);
          SamplingFlags samplingFlags = result.context();
          if (samplingFlags == null) samplingFlags = result.traceIdContext();
          if (samplingFlags == null) samplingFlags = result.samplingFlags();
          if (SamplingFlags.EMPTY != samplingFlags) {
            return result;
          }
        }
        return TraceContextOrSamplingFlags.EMPTY;
      }
    };
  }

  static final class Factory extends Propagation.Factory {
    final List<Propagation.Factory> propagationFactories;
    final boolean injectAll;

    Factory(FactoryBuilder builder) {
      propagationFactories = new ArrayList<>(builder.propagationFactories);
      injectAll = builder.injectAll;
    }

    @Override
    public boolean supportsJoin() {
      for (Propagation.Factory factory : propagationFactories) {
        if (!factory.supportsJoin()) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean requires128BitTraceId() {
      for (Propagation.Factory factory : propagationFactories) {
        if (factory.requires128BitTraceId()) {
          return true;
        }
      }
      return false;
    }

    /**
     * @deprecated Since 5.12, use {@link #get()}
     */
    @Deprecated
    @Override public <K> Propagation<K> create(KeyFactory<K> keyFactory) {
      List<Propagation<K>> propagations = new ArrayList<>();
      for (Propagation.Factory factory : propagationFactories) {
        propagations.add(factory.create(keyFactory));
      }
      return new CompositePropagation<>(propagations, injectAll);
    }

    @Override
    public Propagation<String> get() {
      List<Propagation<String>> propagations = new ArrayList<>();
      for (Propagation.Factory factory : propagationFactories) {
        propagations.add(factory.get());
      }
      return new CompositePropagation<>(propagations, injectAll);
    }

    @Override
    public TraceContext decorate(TraceContext context) {
      for (Propagation.Factory factory : propagationFactories) {
        context = factory.decorate(context);
      }
      return context;
    }

    @Override public String toString() {
      StringBuilder stringBuilder = new StringBuilder("CompositePropagationFactory{inject");
      stringBuilder.append(injectAll ? "All" : "First").append(':');
      for (int i = 0; i < propagationFactories.size(); ++i) {
        stringBuilder.append(propagationFactories.get(i));
        if (i < propagationFactories.size() - 1) stringBuilder.append(',');
      }
      return stringBuilder.append('}').toString();
    }
  }
}
