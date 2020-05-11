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
package brave.internal.propagation;

import brave.Request;
import brave.Span;
import brave.propagation.Propagation;
import brave.propagation.Propagation.RemoteSetter;
import brave.propagation.Propagation.Setter;
import brave.propagation.TraceContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * This is an internal type used to implement possibly multiple trace formats based on {@link
 * Span.Kind}. It isn't intended to be shared widely as it may change in practice.
 *
 * <p>When {@link RemoteSetter} is implemented, {@link #newInjector(Setter)} will create an
 * injector that is pre-configured for {@link RemoteSetter#spanKind()}. Otherwise, it will create an
 * injector that defers until {@link Injector#inject(TraceContext, Object)} is called.
 *
 * <p>A deferred injector checks if the {@code request} parameter is an instance of {@link
 * Request}, it considers {@link Request#spanKind()}. If so, it uses this for injection formats
 * accordingly. If not, a {@linkplain Builder#injectorFunction(InjectorFunction...) default
 * function} is used.
 *
 * <p><em>Note</em>: Instrumentation that have not been recently updated may be remote, but neither
 * implement {@link RemoteSetter} nor {@link Request}. In other words, lack of these types do not
 * mean the input is for a local span. However, this can be assumed for code in this repository, as
 * it is all up-to-date.
 *
 * <p>In Brave 6, we might make the assumption that local injection is when the inputs neither
 * implement {@link RemoteSetter} nor {@link Request}.
 */
public final class InjectorFactory {
  /** Like {@link TraceContext.Injector}, except the {@link Setter} is a parameter. */
  public interface InjectorFunction {
    InjectorFunction NOOP = new InjectorFunction() {
      @Override public List<String> keyNames() {
        return Collections.emptyList();
      }

      @Override public <R> void inject(Setter<R, String> setter, TraceContext context, R request) {
      }
    };

    /**
     * The distinct list of key names this can inject.
     *
     * @see Propagation#keys()
     */
    List<String> keyNames();

    /** Like {@see TraceContext.Injector#inject} except the {@code setter} is explicit. */
    <R> void inject(Setter<R, String> setter, TraceContext context, R request);
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    InjectorFunction injectorFunction = InjectorFunction.NOOP;
    InjectorFunction clientInjectorFunction = InjectorFunction.NOOP;
    InjectorFunction producerInjectorFunction = InjectorFunction.NOOP;
    InjectorFunction consumerInjectorFunction = InjectorFunction.NOOP;

    /**
     * The {@linkplain InjectorFunction injectors} to use when the {@link Setter} is not {@link
     * RemoteSetter} and the request parameter is not a {@link Request}.
     */
    public Builder injectorFunctions(InjectorFunction... injectorFunctions) {
      this.injectorFunction = injectorFunction(injectorFunctions);
      return this;
    }

    /**
     * The {@linkplain InjectorFunction injectors} to use for {@link Span.Kind#CLIENT} contexts,
     * determined by either {@link RemoteSetter#spanKind()} or {@link Request#spanKind()}.
     */
    public Builder clientInjectorFunctions(InjectorFunction... injectorFunctions) {
      this.clientInjectorFunction = injectorFunction(injectorFunctions);
      return this;
    }

    /**
     * The {@linkplain InjectorFunction injectors} to use for {@link Span.Kind#PRODUCER} contexts,
     * determined by either {@link RemoteSetter#spanKind()} or {@link Request#spanKind()}.
     */
    public Builder producerInjectorFunctions(InjectorFunction... injectorFunctions) {
      this.producerInjectorFunction = injectorFunction(injectorFunctions);
      return this;
    }

    /**
     * The {@linkplain InjectorFunction injectors} to use for {@link Span.Kind#CONSUMER} contexts,
     * determined by either {@link RemoteSetter#spanKind()} or {@link Request#spanKind()}.
     *
     * <p>It may seem unusual at first to inject consumers, but this is how the trace context is
     * serialized for message processors that happen on other threads, or distributed stages (like
     * kafka-streams).
     */
    public Builder consumerInjectorFunctions(InjectorFunction... injectorFunctions) {
      this.consumerInjectorFunction = injectorFunction(injectorFunctions);
      return this;
    }

    /** @throws IllegalArgumentException if all builder methods weren't called. */
    public InjectorFactory build() {
      if (injectorFunction == InjectorFunction.NOOP) {
        throw new IllegalArgumentException("injectorFunction == NOOP");
      }
      if (clientInjectorFunction == InjectorFunction.NOOP) {
        throw new IllegalArgumentException("clientInjectorFunction == NOOP");
      }
      if (producerInjectorFunction == InjectorFunction.NOOP) {
        throw new IllegalArgumentException("producerInjectorFunction == NOOP");
      }
      if (consumerInjectorFunction == InjectorFunction.NOOP) {
        throw new IllegalArgumentException("consumerInjectorFunction == NOOP");
      }
      return new InjectorFactory(this);
    }

    Builder() {
    }
  }

  final InjectorFunction injectorFunction;
  final InjectorFunction clientInjectorFunction, producerInjectorFunction, consumerInjectorFunction;
  final List<String> keyNames;

  InjectorFactory(Builder builder) {
    injectorFunction = builder.injectorFunction;
    clientInjectorFunction = builder.clientInjectorFunction;
    producerInjectorFunction = builder.producerInjectorFunction;
    consumerInjectorFunction = builder.consumerInjectorFunction;
    Set<String> keyNames = new LinkedHashSet<>();
    // Add messaging first as their formats are likely the cheapest to extract
    keyNames.addAll(builder.consumerInjectorFunction.keyNames());
    keyNames.addAll(builder.producerInjectorFunction.keyNames());
    keyNames.addAll(builder.clientInjectorFunction.keyNames());
    keyNames.addAll(builder.injectorFunction.keyNames());
    this.keyNames = Collections.unmodifiableList(new ArrayList<>(keyNames));
  }

  /**
   * The distinct list of key names this can inject.
   *
   * @see Propagation#keys()
   */
  public List<String> keyNames() {
    return keyNames;
  }

  /**
   * Creates a potentially composite injector if the input is an instance of {@link RemoteSetter}.
   * Otherwise, a deferred injector is return that examples the request parameter to decide if it is
   * remote or not.
   */
  public <R> TraceContext.Injector<R> newInjector(Setter<R, String> setter) {
    if (setter == null) throw new NullPointerException("setter == null");
    if (setter instanceof RemoteSetter) {
      RemoteSetter<?> remoteSetter = (RemoteSetter<?>) setter;
      switch (remoteSetter.spanKind()) {
        case CLIENT:
          return new Injector<>(setter, clientInjectorFunction);
        case PRODUCER:
          return new Injector<>(setter, producerInjectorFunction);
        case CONSUMER:
          return new Injector<>(setter, consumerInjectorFunction);
        default: // SERVER is nonsense as it cannot be injected
      }
    }
    return new DeferredInjector<>(setter, this);
  }

  @Override public int hashCode() {
    int h = 1000003;
    h ^= injectorFunction.hashCode();
    h *= 1000003;
    h ^= clientInjectorFunction.hashCode();
    h *= 1000003;
    h ^= producerInjectorFunction.hashCode();
    h *= 1000003;
    h ^= consumerInjectorFunction.hashCode();
    return h;
  }

  @Override public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof InjectorFactory)) return false;
    InjectorFactory that = (InjectorFactory) o;
    return injectorFunction.equals(that.injectorFunction)
        && clientInjectorFunction.equals(that.clientInjectorFunction)
        && producerInjectorFunction.equals(that.producerInjectorFunction)
        && consumerInjectorFunction.equals(that.consumerInjectorFunction);
  }

  @Override public String toString() {
    return "InjectorFactory{injectorFunction=" + injectorFunction
        + ", clientInjectorFunction=" + clientInjectorFunction
        + ", producerInjectorFunction=" + producerInjectorFunction
        + ", consumerInjectorFunction=" + consumerInjectorFunction
        + "}";
  }

  static final class DeferredInjector<R> implements TraceContext.Injector<R> {
    final Setter<R, String> setter;
    final InjectorFactory injectorFactory;

    DeferredInjector(Setter<R, String> setter, InjectorFactory injectorFactory) {
      this.setter = setter;
      this.injectorFactory = injectorFactory;
    }

    @Override public void inject(TraceContext context, R request) {
      if (request instanceof Request) {
        switch (((Request) request).spanKind()) {
          case CLIENT:
            injectorFactory.clientInjectorFunction.inject(setter, context, request);
            return;
          case PRODUCER:
            injectorFactory.producerInjectorFunction.inject(setter, context, request);
            return;
          case CONSUMER:
            injectorFactory.consumerInjectorFunction.inject(setter, context, request);
          default: // SERVER is nonsense as it cannot be injected
        }
      }
      injectorFactory.injectorFunction.inject(setter, context, request);
    }

    @Override public int hashCode() {
      int h = 1000003;
      h ^= setter.hashCode();
      h *= 1000003;
      h ^= injectorFactory.hashCode();
      return h;
    }

    @Override public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof DeferredInjector)) return false;
      DeferredInjector<?> that = (DeferredInjector<?>) o;
      return setter.equals(that.setter) && injectorFactory.equals(that.injectorFactory);
    }

    @Override public String toString() {
      return "DeferredInjector{setter=" + setter + ", injectorFactory=" + injectorFactory + "}";
    }
  }

  static final class Injector<R> implements TraceContext.Injector<R> {
    final InjectorFunction injectorFunction;
    final Setter<R, String> setter;

    Injector(Setter<R, String> setter, InjectorFunction injectorFunction) {
      this.injectorFunction = injectorFunction;
      this.setter = setter;
    }

    @Override public void inject(TraceContext context, R request) {
      injectorFunction.inject(setter, context, request);
    }

    @Override public int hashCode() {
      int h = 1000003;
      h ^= setter.hashCode();
      h *= 1000003;
      h ^= injectorFunction.hashCode();
      return h;
    }

    @Override public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof Injector)) return false;
      Injector<?> that = (Injector<?>) o;
      return setter.equals(that.setter) && injectorFunction.equals(that.injectorFunction);
    }

    @Override public String toString() {
      return "Injector{setter=" + setter + ", injectorFunction=" + injectorFunction + "}";
    }
  }

  static InjectorFunction injectorFunction(InjectorFunction... injectorFunctions) {
    if (injectorFunctions == null) throw new NullPointerException("injectorFunctions == null");
    LinkedHashSet<InjectorFunction> injectorFunctionSet =
        new LinkedHashSet<>(Arrays.asList(injectorFunctions));
    injectorFunctionSet.remove(InjectorFunction.NOOP);
    if (injectorFunctionSet.isEmpty()) return InjectorFunction.NOOP;
    if (injectorFunctionSet.size() == 1) return injectorFunctionSet.iterator().next();
    return new CompositeInjectorFunction(injectorFunctionSet.toArray(new InjectorFunction[0]));
  }

  static final class CompositeInjectorFunction implements InjectorFunction {
    final InjectorFunction[] injectorFunctions; // Array ensures no iterators are created at runtime
    final List<String> keyNames;

    CompositeInjectorFunction(InjectorFunction[] injectorFunctions) {
      this.injectorFunctions = injectorFunctions;
      Set<String> keyNames = new LinkedHashSet<>();
      for (InjectorFunction injectorFunction : injectorFunctions) {
        keyNames.addAll(injectorFunction.keyNames());
      }
      this.keyNames = Collections.unmodifiableList(new ArrayList<>(keyNames));
    }

    @Override public List<String> keyNames() {
      return keyNames;
    }

    @Override public <R> void inject(Setter<R, String> setter, TraceContext context, R request) {
      for (InjectorFunction injectorFunction : injectorFunctions) {
        injectorFunction.inject(setter, context, request);
      }
    }

    @Override public int hashCode() {
      int h = 1000003;
      h ^= Arrays.hashCode(injectorFunctions);
      return h;
    }

    @Override public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof CompositeInjectorFunction)) return false;
      return Arrays.equals(injectorFunctions, ((CompositeInjectorFunction) o).injectorFunctions);
    }

    @Override public String toString() {
      return Arrays.toString(injectorFunctions);
    }
  }
}
