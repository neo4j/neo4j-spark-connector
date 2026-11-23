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
