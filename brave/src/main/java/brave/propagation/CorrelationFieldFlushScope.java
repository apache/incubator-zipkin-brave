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

import brave.internal.CorrelationContext;
import brave.propagation.CorrelationField.Updatable;
import brave.propagation.CurrentTraceContext.Scope;
import java.util.ArrayDeque;
import java.util.LinkedHashSet;
import java.util.Set;

import static brave.propagation.CorrelationScopeDecorator.equal;
import static brave.propagation.CorrelationScopeDecorator.update;

/** Sets up thread locals if they are needed to support {@link Updatable#flushOnUpdate()} */
final class CorrelationFieldFlushScope implements Scope {
  final CorrelationFieldUpdateScope updateScope;

  CorrelationFieldFlushScope(CorrelationFieldUpdateScope updateScope) {
    this.updateScope = updateScope;
    pushCurrentUpdateScope(updateScope);
  }

  @Override public void close() {
    popCurrentUpdateScope(updateScope);
    updateScope.close();
  }

  /**
   * Handles a flush by synchronizing the correlation context followed by signaling each stacked
   * scope about a potential field update.
   *
   * <p>Overhead here occurs on the calling thread. Ex. the one that calls {@link
   * BaggageField#updateValue(String)}.
   */
  static void flush(Updatable field, String value) {
    assert field.flushOnUpdate();

    Set<CorrelationContext> syncedContexts = new LinkedHashSet<>();
    for (Object o : updateScopeStack()) {
      CorrelationFieldUpdateScope next = ((CorrelationFieldUpdateScope) o);

      // Since this is a static method, it could be called with different tracers on the stack.
      // This synchronizes the context if we haven't already.
      if (!syncedContexts.contains(next.context)) {
        if (!equal(next.context.get(field.name()), value)) {
          update(next.context, field, value);
        }
        syncedContexts.add(next.context);
      }

      // Now, signal the current scope in case it has a value change
      next.handleUpdate(field, value);
    }
  }

  static final ThreadLocal<ArrayDeque<Object>> updateScopeStack = new ThreadLocal<>();

  static ArrayDeque<Object> updateScopeStack() {
    ArrayDeque<Object> stack = updateScopeStack.get();
    if (stack == null) {
      stack = new ArrayDeque<>();
      updateScopeStack.set(stack);
    }
    return stack;
  }

  static void pushCurrentUpdateScope(CorrelationFieldUpdateScope updateScope) {
    updateScopeStack().push(updateScope);
  }

  static void popCurrentUpdateScope(CorrelationFieldUpdateScope expected) {
    Object popped = updateScopeStack().pop();
    assert equal(popped, expected) :
      "Misalignment: popped updateScope " + popped + " !=  expected " + expected;
  }
}
