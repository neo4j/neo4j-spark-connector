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
package org.neo4j.spark.testsupport;//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.fail;

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
            fail("Expected exception of type " + typeOfException + ", but no exception was thrown");
        } catch (Exception var4) {
            if (typeOfException.isInstance(var4)) {
                if (partOfErrorMessage != null) {
                    Assertions.assertTrue(var4.getMessage() != null  && var4.getMessage().contains(partOfErrorMessage), "Expected exception message to be present");
                }
            } else {
                fail("Got unexpected exception " + var4.getClass() + "\nExpected: " + typeOfException);
            }
        }

    }

    public static <T, E extends Exception> void assertEventually(T expected, ThrowingSupplier<T, E> actual, long timeout, TimeUnit timeUnit) throws E, InterruptedException {
        assertEventually((ignored) -> "", actual, expected, timeout, timeUnit);
    }

    public static <T, E extends Exception> void assertEventually(Function<T, String> reason, ThrowingSupplier<T, E> actual, T expected, long timeout, TimeUnit timeUnit) throws E, InterruptedException {
        long endTimeMillis = System.currentTimeMillis() + timeUnit.toMillis(timeout);

        while (true) {
            long sampleTime = System.currentTimeMillis();
            T last = actual.get();
            boolean matched = Objects.equals(expected, last);
            if (matched || sampleTime > endTimeMillis) {
                if (!matched) {
                    throw new AssertionError(
                        "Timeout hit (" + timeout + " " + timeUnit.toString().toLowerCase()
                            + ") while waiting for condition to match: " + reason.apply(last)
                            + "\nExpected: " + prettyPrint(expected)
                            + "\n     but: " + prettyPrint(last));
                } else {
                    return;
                }
            }

            Thread.sleep(100L);
        }
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

