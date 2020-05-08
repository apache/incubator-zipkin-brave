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
package brave.internal.codec;

import brave.internal.Platform;

/**
 * Splits a character sequence that's in a delimited string, optionally trimming optional whitespace
 * (<a href="https://httpwg.org/specs/rfc7230.html#rfc.section.3.2">OWS</a>) before or after
 * delimiters.
 *
 * <p>This is intended to be initialized as a constant, as doing so per-request will add
 * unnecessary overhead.
 */
public final class EntrySplitter {
  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    int maxEntries = Integer.MAX_VALUE;
    char entrySeparator = ',', keyValueSeparator = '=';
    boolean trimOWSAroundEntrySeparator = true, trimOWSAroundKeyValueSeparator = true;
    boolean keyValueSeparatorRequired = true, shouldThrow = false;

    /**
     * When set, {@link Handler} will be called maximum {@code maxEntries} times per parse. After
     * that, {@link #parse(Handler, Object, String)} returns false or throws an exception, based on
     * {@link #shouldThrow(boolean)}. Default: {@link Integer#MAX_VALUE}.
     *
     * <p>This is used to implement strict format constraints. For example, above 32 entries is
     * malformed. This is separate from any capacity constraints of the {@link Handler}, which may
     * be smaller than this amount.
     */
    public Builder maxEntries(int maxEntries) {
      if (maxEntries <= 0) throw new IllegalArgumentException("maxEntries <= 0");
      this.maxEntries = maxEntries;
      return this;
    }

    /**
     * The separator to use between entries. For example, given a string "k1=v1,k2=v2", the {@code
     * entrySeparator} should be ','. Given a string "k1=v1;k2=v2" the {@code entrySeparator} should
     * be ';'. Default: ','
     *
     * @see #keyValueSeparator(char)
     */
    public Builder entrySeparator(char entrySeparator) {
      if (entrySeparator == 0) throw new IllegalArgumentException("entrySeparator == 0");
      if (entrySeparator == keyValueSeparator) {
        throw new IllegalArgumentException("entrySeparator == keyValueSeparator");
      }
      this.entrySeparator = entrySeparator;
      return this;
    }

    /**
     * The separator to use between a key and value. For example, given a string "k1=v1,k2=v2", the
     * {@code keyValueSeparator} should be '='. Default: '='
     *
     * <p><em>Note:</em> Only the first {@code keyValueSeparator} identifies the end of the key
     * until the next {@link #entrySeparator(char)}. This means values can include the {@code
     * keyValueSeparator} character.
     *
     * <p>For example, the string "condition=animal=cat", with
     * {@code keyValueSeparator} '=' parses {@code [("condition", "animal=cat7)]}
     *
     * @see #keyValueSeparator(char)
     */
    public Builder keyValueSeparator(char keyValueSeparator) {
      if (keyValueSeparator == 0) throw new IllegalArgumentException("keyValueSeparator == 0");
      if (keyValueSeparator == entrySeparator) {
        throw new IllegalArgumentException("keyValueSeparator == entrySeparator");
      }
      this.keyValueSeparator = keyValueSeparator;
      return this;
    }

    /**
     * When {@code true}, optional whitespace (spaces and tabs aka <a href="https://httpwg.org/specs/rfc7230.html#rfc.section.3.2">OWS</a>)
     * are removed around the {@link #entrySeparator} and string boundaries. Default: {@code true}
     *
     * <p>For example, given the string "  k1   =   v1  ,  k2   =   v2  ", this trims around the
     * "=" character and string boundaries: {@code [("k1   ","   v1"),("k2   ", "   v2")]}.
     *
     * @see #trimOWSAroundKeyValueSeparator(boolean)
     */
    public Builder trimOWSAroundEntrySeparator(boolean trimOWSAroundEntrySeparator) {
      this.trimOWSAroundEntrySeparator = trimOWSAroundEntrySeparator;
      return this;
    }

    /**
     * When {@code true}, optional whitespace (spaces and tabs aka <a href="https://httpwg.org/specs/rfc7230.html#rfc.section.3.2">OWS</a>)
     * are removed around the {@link #keyValueSeparator(char)}. Default: {@code true}
     *
     * <p>For example, given the string "  k1   =   v1  ,  k2   =   v2  ", this trims around the
     * "=" character and string boundaries: {@code [("  k1", "v1  "),("  k2", "v2  ")]}.
     *
     * @see #trimOWSAroundKeyValueSeparator(boolean)
     */
    public Builder trimOWSAroundKeyValueSeparator(boolean trimOWSAroundKeyValueSeparator) {
      this.trimOWSAroundKeyValueSeparator = trimOWSAroundKeyValueSeparator;
      return this;
    }

    /**
     * When {@code true}, when a {@link #keyValueSeparator(char)} does not follow a key, {@link
     * #parse(Handler, Object, String)} returns false or throws an exception, based on {@link
     * #shouldThrow(boolean)}. Default: {@code true}.
     *
     * <p>Setting this to {@code false} makes "k1,k2=v2" interpreted the same as if there was
     * a {@link #keyValueSeparator(char)}: "k1=,k2=v2". This is used for formats such as HTTP
     * queries where separators are optional.
     */
    public Builder keyValueSeparatorRequired(boolean keyValueSeparatorRequired) {
      this.keyValueSeparatorRequired = keyValueSeparatorRequired;
      return this;
    }

    /**
     * On validation fail, should this throw an exception or log?. The use case to throw is when
     * validating input (ex into a builder), or in unit tests.
     */
    public Builder shouldThrow(boolean shouldThrow) {
      this.shouldThrow = shouldThrow;
      return this;
    }

    public EntrySplitter build() {
      return new EntrySplitter(this);
    }
  }

  /**
   * This is a callback on offsets to avoid allocating strings for a malformed input {@code input}.
   *
   * @param <T> target of parsed entries
   */
  public interface Handler<T> {
    /**
     * Called for each valid entry split from the input {@code input}. Return {@code false} after
     * logging to stop due to invalid input.
     *
     * <p>After validating, typically strings will be parsed from the input like so:
     * <pre>{@code
     * String key = input.substring(beginKey, endKey);
     * String value = input.substring(beginValue, endValue);
     * }</pre>
     *
     * @param target receiver of parsed entries
     * @param input string including data to parse
     * @param beginKey begin index of the entry's key in {@code input}, inclusive
     * @param endKey end index of the entry's key in {@code input}, exclusive
     * @param beginValue begin index of the entry's value in {@code input}, inclusive
     * @param endValue end index of the entry's value in {@code input}, exclusive
     * @return true if we reached the {@code endIndex} without failures.
     */
    boolean onEntry(
        T target, String input, int beginKey, int endKey, int beginValue, int endValue);
  }

  final char keyValueSeparator, entrySeparator;
  int maxEntries;
  final boolean trimOWSAroundEntrySeparator, trimOWSAroundKeyValueSeparator;
  final boolean keyValueSeparatorRequired, shouldThrow;
  final String missingKey, missingKeyValueSeparator, overMaxEntries;

  EntrySplitter(Builder builder) {
    keyValueSeparator = builder.keyValueSeparator;
    entrySeparator = builder.entrySeparator;
    maxEntries = builder.maxEntries;
    trimOWSAroundEntrySeparator = builder.trimOWSAroundEntrySeparator;
    trimOWSAroundKeyValueSeparator = builder.trimOWSAroundKeyValueSeparator;
    keyValueSeparatorRequired = builder.keyValueSeparatorRequired;
    shouldThrow = builder.shouldThrow;
    missingKey = "Invalid input: no key before '" + keyValueSeparator + "'";
    missingKeyValueSeparator =
        "Invalid input: missing key value separator '" + keyValueSeparator + "'";
    overMaxEntries = "Invalid input: over " + maxEntries + " entries";
  }

  /**
   * @param handler parses entries emitted upon success
   * @param target receiver of parsed entries
   * @param input string including data to parse
   * @return true if we reached the {@code endIndex} without failures.
   */
  public <T> boolean parse(Handler<T> handler, T target, String input) {
    if (input == null) throw new NullPointerException("input == null");
    return parse(handler, target, input, 0, input.length());
  }

  /**
   * @param handler parses entries emitted upon success
   * @param target receiver of parsed entries
   * @param input string including data to parse
   * @param beginIndex begin index of the {@code input}, inclusive
   * @param endIndex end index of the {@code input}, exclusive
   * @return true if we reached the {@code endIndex} without failures.
   */
  public <T> boolean parse(
      Handler<T> handler, T target, String input, int beginIndex, int endIndex) {
    if (handler == null) throw new NullPointerException("handler == null");
    if (target == null) throw new NullPointerException("target == null");
    if (input == null) throw new NullPointerException("input == null");
    if (beginIndex < 0) throw new IllegalArgumentException("beginIndex < 0");
    if (endIndex > input.length()) throw new IllegalArgumentException("endIndex > input.length()");

    int remainingEntries = maxEntries, beginKey = -1, endKey = -1, beginValue = -1;
    for (int i = beginIndex; i < endIndex; i++) {
      char c = input.charAt(i);

      boolean nextIsEnd = i + 1 == endIndex;
      if (c == entrySeparator || nextIsEnd) { // finished an entry
        if (c == keyValueSeparator) {
          beginValue = i; // empty value: ex "key=" "k1 ="
        }

        if (beginKey == -1 && beginValue == -1) {
          continue; // ignore empty entries, like ",,"
        } else if (beginKey == -1) {
          return logOrThrow(missingKey, shouldThrow); // ex. "=" ",="
        } else if (nextIsEnd && beginValue == -1) { // ex "k1" "k1 " "a=b" "..=,"
          // We reached the end of a key-only entry, a single character entry or an empty entry
          beginValue = c == entrySeparator ? i + 1 : i;
        }

        int endValue;
        if (endKey == -1) {
          if (keyValueSeparatorRequired && c != keyValueSeparator) {
            return logOrThrow(missingKeyValueSeparator, shouldThrow); // throw on "k1" "k1=v2,k2"
          }

          // Even though we have an empty value, we need to handle whitespace and
          // boundary conditions.
          //
          // For example, using entry separator ',' and KV separator '=':
          // "...,k1" and input[i] == 'y', we want i + 1, so that the key includes the 'y'
          // "...,k1 " and input[i] == ' ', we want i + 1, as the key includes a trailing ' '
          // "...,k1=" and input[i] == '=', we want i, bc a KV separator isn't part of the key
          // "k1 , k2" and input[i] == ',', we want i, bc an entry separator isn't part of the key
          endKey = nextIsEnd && c != keyValueSeparator ? i + 1 : i;

          if (trimOWSAroundKeyValueSeparator) {
            endKey = rewindOWS(input, beginKey, endKey);
          }
          beginValue = endValue = endKey; // value is empty
        } else {
          endValue = nextIsEnd ? i + 1 : i;

          if (trimOWSAroundEntrySeparator) {
            endValue = rewindOWS(input, beginValue, endValue);
          }
        }

        if (remainingEntries-- == 0) logOrThrow(overMaxEntries, shouldThrow);

        if (!handler.onEntry(target, input, beginKey, endKey, beginValue, endValue)) {
          return false; // assume handler logs
        }

        beginKey = endKey = beginValue = -1; // reset for the next entry
      } else if (beginKey == -1) {
        if (trimOWSAroundEntrySeparator && isOWS(c)) continue; // skip whitespace before key
        if (c == keyValueSeparator) {
          if (i == beginIndex || input.charAt(i - 1) == entrySeparator) {
            return logOrThrow(missingKey, shouldThrow); // ex "=v1" ",=v2"
          }
        }
        beginKey = i;
      } else if (endKey == -1 && c == keyValueSeparator) { // only use the first separator for key
        endKey = i;
        if (trimOWSAroundKeyValueSeparator) {
          endKey = rewindOWS(input, beginIndex, endKey);
        }
      } else if (endKey != -1 && beginValue == -1) { // only look for a value if you have a key
        if (trimOWSAroundKeyValueSeparator && isOWS(c)) continue; // skip whitespace before value
        if (c == keyValueSeparator) continue; // skip the keyValueSeparator (ex '=')
        beginValue = i;
      }
    }
    return true;
  }

  static int rewindOWS(String input, int beginIndex, int endIndex) {
    // endIndex is a boundary. we need to begin looking one character before it.
    while (isOWS(input.charAt(endIndex - 1))) {
      if (--endIndex == beginIndex) return beginIndex; // trim whitespace
    }
    return endIndex;
  }

  // OWS is zero or more spaces or tabs https://httpwg.org/specs/rfc7230.html#rfc.section.3.2
  static boolean isOWS(char c) {
    return c == ' ' || c == '\t';
  }

  static boolean logOrThrow(String msg, boolean shouldThrow) {
    if (shouldThrow) throw new IllegalArgumentException(msg);
    Platform.get().log(msg, null);
    return false;
  }
}
