package org.neo4j.spark.util;

import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Optional;
import java.util.stream.Stream;

public class ReflectionUtils {

    private static final MethodHandles.Lookup lookup = MethodHandles.lookup();

    private static Optional<MethodHandle> getGroupByColumns() {
        try {
            return Optional.of(lookup
                    .findVirtual(Aggregation.class, "groupByColumns", MethodType.methodType(NamedReference[].class))
                    .asType(MethodType.methodType(Expression[].class, Aggregation.class)));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static Optional<MethodHandle> getGroupByExpressions() {
        try {
            return Optional.of(lookup
                    .findVirtual(Aggregation.class, "groupByExpressions", MethodType.methodType(Expression[].class)));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static final Optional<MethodHandle> groupByColumns = getGroupByColumns();
    private static final Optional<MethodHandle> groupByExpressions = getGroupByExpressions();

    private static final Expression[] EMPTY = new Expression[0];

    public static Expression[] groupByCols(Aggregation agg) {
        return Stream.of(groupByExpressions, groupByColumns)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(mh -> {
                    try {
                        return (Expression[]) mh.invokeExact(agg);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        return EMPTY;
                    }
                })
                .findFirst()
                .orElse(EMPTY);
    }

}