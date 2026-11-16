package org.neo4j.spark.ingest.utils;

import java.util.List;
import java.util.stream.Collectors;
import org.neo4j.importer.v1.targets.PropertyMapping;

public final class SpecToSparkUtils {

    private SpecToSparkUtils() {}

    public static String getLabels(List<String> labels) {
        return labels.isEmpty() ? "" : ":" + String.join(":", labels);
    }

    public static String getKeys(List<PropertyMapping> keys) {
        return keys.stream()
                .map(key -> key.getSourceField() + ":" + key.getTargetProperty())
                .collect(Collectors.joining(","));
    }
}
