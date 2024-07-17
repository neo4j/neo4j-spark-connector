/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.spark;//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.StringDescription;
import org.hamcrest.core.StringContains;

public final class Assert {
    private Assert() {
    }

    public interface ThrowingSupplier<T, E extends Exception> {
        T get() throws E;

        static <TYPE> ThrowingSupplier<TYPE, RuntimeException> throwingSupplier(final Supplier<TYPE> supplier) {
            return new ThrowingSupplier<TYPE, RuntimeException>() {
                public TYPE get() {
                    return supplier.get();
                }

                public String toString() {
                    return supplier.toString();
                }
            };
        }
    }

    public interface ThrowingAction<E extends Exception> {
        void apply() throws E;

        static <E extends Exception> ThrowingAction<E> noop() {
            return () -> {
            };
        }
    }

    public static <E extends Exception> void assertException(ThrowingAction<E> f, Class<?> typeOfException) {
        assertException(f, typeOfException, (String) null);
    }

    public static <E extends Exception> void assertException(ThrowingAction<E> f, Class<?> typeOfException, String partOfErrorMessage) {
        try {
            f.apply();
            org.junit.Assert.fail("Expected exception of type " + typeOfException + ", but no exception was thrown");
        } catch (Exception var4) {
            if (typeOfException.isInstance(var4)) {
                if (partOfErrorMessage != null) {
                    MatcherAssert.assertThat(var4.getMessage(), StringContains.containsString(partOfErrorMessage));
                }
            } else {
                org.junit.Assert.fail("Got unexpected exception " + var4.getClass() + "\nExpected: " + typeOfException);
            }
        }

    }

    public static <T, E extends Exception> void assertEventually(ThrowingSupplier<T, E> actual, Matcher<? super T> matcher, long timeout, TimeUnit timeUnit) throws E, InterruptedException {
        assertEventually((ignored) -> {
            return "";
        }, actual, matcher, timeout, timeUnit);
    }

    public static <T, E extends Exception> void assertEventually(String reason, ThrowingSupplier<T, E> actual, Matcher<? super T> matcher, long timeout, TimeUnit timeUnit) throws E, InterruptedException {
        assertEventually((ignored) -> {
            return reason;
        }, actual, matcher, timeout, timeUnit);
    }

    public static <T, E extends Exception> void assertEventually(Function<T, String> reason, ThrowingSupplier<T, E> actual, Matcher<? super T> matcher, long timeout, TimeUnit timeUnit) throws E, InterruptedException {
        long endTimeMillis = System.currentTimeMillis() + timeUnit.toMillis(timeout);

        while (true) {
            long sampleTime = System.currentTimeMillis();
            T last = actual.get();
            boolean matched = matcher.matches(last);
            if (matched || sampleTime > endTimeMillis) {
                if (!matched) {
                    Description description = new StringDescription();
                    description.appendText((String) reason.apply(last)).appendText("\nExpected: ").appendDescriptionOf(matcher).appendText("\n     but: ");
                    matcher.describeMismatch(last, description);
                    throw new AssertionError("Timeout hit (" + timeout + " " + timeUnit.toString().toLowerCase() + ") while waiting for condition to match: " + description.toString());
                } else {
                    return;
                }
            }

            Thread.sleep(100L);
        }
    }

    private static AssertionError newAssertionError(String message, Object expected, Object actual) {
        return new AssertionError((message != null && !message.isEmpty() ? message + "\n" : "") + "Expected: " + prettyPrint(expected) + ", actual: " + prettyPrint(actual));
    }

    private static String prettyPrint(Object o) {
        if (o == null) {
            return "null";
        }

        Class<?> clazz = o.getClass();
        if (clazz.isArray()) {
            if (clazz == byte[].class) {
                return Arrays.toString((byte[]) o);
            } else if (clazz == short[].class) {
                return Arrays.toString((short[]) o);
            } else if (clazz == int[].class) {
                return Arrays.toString((int[]) o);
            } else if (clazz == long[].class) {
                return Arrays.toString((long[]) o);
            } else if (clazz == float[].class) {
                return Arrays.toString((float[]) o);
            } else if (clazz == double[].class) {
                return Arrays.toString((double[]) o);
            } else if (clazz == char[].class) {
                return Arrays.toString((char[]) o);
            } else if (clazz == boolean[].class) {
                return Arrays.toString((boolean[]) o);
            } else {
                return Arrays.deepToString((Object[]) o);
            }
        } else {
            return String.valueOf(o);
        }
    }
}

