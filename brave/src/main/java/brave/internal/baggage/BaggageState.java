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
package brave.internal.baggage;

import brave.baggage.BaggageField;
import brave.internal.Nullable;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Holds one or more baggage fields detached from a {@linkplain TraceContext}.
 *
 * <p>We need to retain propagation state extracted from headers. However, we don't know the trace
 * identifiers, yet. In order to resolve this ordering concern, we create an object to hold extra
 * state, and defer associating it with a span ID (via {@link BaggageStateFactory#decorate(TraceContext)}.
 *
 * <p>The implementation of this type uses copy-on-write semantics to prevent changes in a
 * child context from affecting its parent.
 */
public final class BaggageState {
  public static Factory newFactory(BaggageStateHandler... handlers) {
    return new BaggageStateFactory(handlers);
  }

  public interface Factory {
    BaggageState create();

    BaggageState create(BaggageState parent);

    TraceContext decorate(TraceContext context);
  }

  /** The list of fields present, regardless of value. */
  public List<BaggageField> getFields() {
    List<BaggageField> result = new ArrayList<>();
    Object[] stateArray = this.stateArray;
    for (int i = 0, length = handlers.length; i < length; i++) {
      result.addAll(handlers[i].currentFields(stateArray != null ? stateArray[i] : null));
    }
    return Collections.unmodifiableList(result);
  }

  /**
   * Returns the value of the field with the specified name or null if not available.
   *
   * @see BaggageField#getValue(TraceContext)
   */
  @Nullable public String getValue(BaggageField field) {
    int index = indexOf(field);
    if (index == -1) return null;
    Object[] stateArray = this.stateArray;
    if (stateArray == null) return null;
    Object state = stateArray[index];
    return state != null ? handlers[index].getValue(field, state) : null;
  }

  /**
   * Replaces the value of the field with the specified key, ignoring if not configured.
   *
   * @see BaggageField#updateValue(TraceContext, String)
   */
  public boolean updateValue(BaggageField field, String value) {
    int index = indexOf(field);
    if (index == -1) return false;

    BaggageStateHandler handler = handlers[index];
    synchronized (this) {
      Object[] stateArray = this.stateArray;
      if (stateArray == null) {
        if (value == null) return false;
        stateArray = new Object[handlers.length];
        stateArray[index] = handler.newState(field, value);
        this.stateArray = stateArray;
        return true;
      }

      Object state;
      if (stateArray[index] == null) {
        state = handler.newState(field, value);
      } else {
        state = handler.mergeState(stateArray[index], field, value);
      }

      if (equal(state, stateArray[index])) return false;

      // this is the copy-on-write part
      stateArray = Arrays.copyOf(stateArray, stateArray.length);
      stateArray[index] = state;
      this.stateArray = stateArray;
      return true;
    }
  }

  /**
   * Returns true if it can decode values for this baggage handler.
   *
   * @see Propagation.Getter
   */
  public boolean decodeState(BaggageStateHandler handler, String encoded) {
    if (handler == null) throw new NullPointerException("handler == null");
    if (encoded == null) throw new NullPointerException("encoded == null");

    int index = indexOf(handler);
    if (index == -1) return false;

    Object state = handlers[index].decode(encoded);
    if (state == null) return false;
    // Unsynchronized as only called during extraction when the object is new.
    putState(index, state);
    return true;
  }

  /**
   * Returns encoded state of any value associated with this baggage handler.
   *
   * @see Propagation.Setter
   */
  @Nullable public String encodeState(BaggageStateHandler handler) {
    if (handler == null) throw new NullPointerException("handler == null");

    int index = indexOf(handler);
    if (index == -1) return null;

    Object maybeValue = getState(index);
    if (maybeValue == null) return null;
    return handlers[index].encode(maybeValue);
  }

  final BaggageStateHandler[] handlers;
  volatile Object[] stateArray; // guarded by this, copy on write
  long traceId, spanId; // guarded by this

  BaggageState(BaggageStateHandler[] handlers) {
    this.handlers = handlers;
  }

  BaggageState(BaggageState parent, BaggageStateHandler[] handlers) {
    this(handlers);
    checkSameHandlers(parent);
    this.stateArray = parent.stateArray;
  }

  boolean putState(int index, @Nullable Object state) {
    Object[] stateArray = this.stateArray;
    if (stateArray == null) {
      stateArray = new Object[handlers.length];
      stateArray[index] = state;
    } else if (equal(state, stateArray[index])) {
      return false;
    } else { // this is the copy-on-write part
      stateArray = Arrays.copyOf(stateArray, stateArray.length);
      stateArray[index] = state;
    }
    this.stateArray = stateArray;
    return true;
  }

  void checkSameHandlers(BaggageState predefinedParent) {
    if (!Arrays.equals(handlers, predefinedParent.handlers)) {
      throw new IllegalStateException(
        String.format("Mixed name configuration unsupported: found %s, expected %s",
          Arrays.toString(handlers), Arrays.toString(predefinedParent.handlers))
      );
    }
  }

  int indexOf(BaggageStateHandler handler) {
    for (int i = 0, length = handlers.length; i < length; i++) {
      if (handlers[i].equals(handler)) return i;
    }
    return -1;
  }

  int indexOf(BaggageField field) {
    for (int i = 0, length = handlers.length; i < length; i++) {
      if (handlers[i].handlesField(field)) return i;
    }
    return -1;
  }

  @Nullable Object getState(int index) {
    Object[] stateArray = this.stateArray;
    if (stateArray == null || index >= stateArray.length) return null;
    return stateArray[index];
  }

  /**
   * For each field in the input replace the state if the key doesn't already exist.
   *
   * <p>Note: this does not synchronize internally as it is acting on newly constructed fields
   * not yet returned to a caller.
   */
  void putAllIfAbsent(BaggageState parent) {
    checkSameHandlers(parent);
    Object[] parentStateArray = parent.stateArray;
    if (parentStateArray == null) return;
    for (int i = 0; i < parentStateArray.length; i++) {
      if (parentStateArray[i] != null && getState(i) == null) { // extracted wins vs parent
        putState(i, parentStateArray[i]);
      }
    }
  }

  /** Fields are extracted before a context is created. We need to lazy set the context */
  boolean tryToClaim(long traceId, long spanId) {
    synchronized (this) {
      if (this.traceId == 0L) {
        this.traceId = traceId;
        this.spanId = spanId;
        return true;
      }
      return this.traceId == traceId && this.spanId == spanId;
    }
  }

  // Implemented for equals when no baggage was extracted
  @Override public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof BaggageState)) return false;
    BaggageState that = (BaggageState) o;
    return Arrays.equals(handlers, that.handlers) && Arrays.equals(stateArray, that.stateArray);
  }

  @Override public int hashCode() {
    int h = 1000003;
    h ^= Arrays.hashCode(handlers);
    h *= 1000003;
    h ^= Arrays.hashCode(stateArray);
    return h;
  }

  static boolean equal(@Nullable Object a, @Nullable Object b) {
    return a == null ? b == null : a.equals(b); // Java 6 can't use Objects.equals()
  }
}
