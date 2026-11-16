package org.neo4j.spark.ingest.utils;

import java.util.List;
import java.util.stream.Collectors;
import org.neo4j.importer.v1.pipeline.NodeTargetStep;
import org.neo4j.importer.v1.pipeline.RelationshipTargetStep;
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

    public static String getNodePattern(NodeTargetStep node) {
        if (node == null) return "";
        return "(" + getLabels(node.labels()) + ")";
    }

    public static String getRelationshipPattern(RelationshipTargetStep relationship) {
        if (relationship == null) return "";
        return getNodePattern(relationship.startNode()) + "-[:" + relationship.type() + "]->"
                + getNodePattern(relationship.endNode());
    }
}
