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
package org.neo4j.spark.ingest;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.pipeline.*;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.spark.ingest.planner.DiagonalPartitionPlanner;
import org.neo4j.spark.ingest.planner.PartitionPlanner;
import org.neo4j.spark.ingest.planner.RoundRobinPartitionPlanner;
import org.neo4j.spark.ingest.utils.SpecToSparkUtils;

public class Neo4jIngestionPlan {
    private final Map<String, String> configuration;
    private final HashMap<String, Dataset<Row>> datasets = new HashMap<>();
    private final SparkSession spark;
    private final ImportSpecification spec;
    private final int workers;

    public static final String FORMAT = "org.neo4j.spark.DataSource";

    public Neo4jIngestionPlan(SparkSession spark, ImportSpecification spec, Map<String, String> configuration) {
        this.spark = spark;
        this.spec = spec;
        this.configuration = configuration;

        Objects.requireNonNull(
                configuration.get("url"), "Must specify 'url' targeting a Neo4j instance in your config");
        Objects.requireNonNull(configuration.get("workers"), "Must specify 'workers' (int) in your config");
        this.workers = Integer.parseInt(configuration.get("workers"));
    }

    public void ingest() {
        ingest(false);
    }

    public void ingest(boolean isDisjoint) {
        ingest(isDisjoint, ImportPipeline.of(spec).executionPlan());
    }

    private void ingest(boolean isDisjoint, ImportExecutionPlan importExecutionPlan) {
        for (var importStepGroup : importExecutionPlan.getGroups()) {
            for (var importStepStage : importStepGroup.getStages()) {
                for (var importStep : importStepStage.getSteps()) {
                    if (importStep instanceof SourceStep sourceStep) {
                        indexSource(sourceStep);
                    } else if (importStep instanceof NodeTargetStep node) {
                        ingestNode(node);
                    } else if (importStep instanceof RelationshipTargetStep relationship) {
                        ingestRelationship(relationship, isDisjoint);
                    } else {
                        throw new IllegalArgumentException("Unsupported step: " + importStep);
                    }
                }
            }
        }
    }

    private void indexSource(SourceStep sourceStep) {
        if (!(sourceStep.source() instanceof SparkSource sparkSource)) {
            throw new IllegalArgumentException("Unsupported source: " + sourceStep.name());
        }

        datasets.put(sparkSource.getName(), sparkSource.getData());
    }

    private void ingestNode(NodeTargetStep node) {
        SparkSession.setActiveSession(spark);
        spark.sparkContext().setCallSite("Neo4j Ingestion " + SpecToSparkUtils.getNodePattern(node));

        applyConfigurationOptions(
                        datasets.get(node.sourceName()).repartition(workers).write())
                .mode(SpecToSparkUtils.writeMode(node.writeMode()))
                .format(FORMAT)
                .option("labels", SpecToSparkUtils.getLabels(node.labels()))
                .option("node.keys", SpecToSparkUtils.getKeys(node.keyProperties()))
                .save();
    }

    private void ingestRelationship(RelationshipTargetStep relationship, boolean isDisjoint) {
        SparkSession.setActiveSession(spark);
        spark.sparkContext().setCallSite("Neo4j Ingestion " + SpecToSparkUtils.getRelationshipPattern(relationship));

        // note: these are arrays because we assume composite keys
        var startColumns = relationship.startNode().keyProperties().stream()
                .map(PropertyMapping::getSourceField)
                .map(functions::col)
                .toArray(Column[]::new);

        var endColumns = relationship.endNode().keyProperties().stream()
                .map(PropertyMapping::getSourceField)
                .map(functions::col)
                .toArray(Column[]::new);

        PartitionPlanner planner;

        if (isDisjoint) {
            planner = new DiagonalPartitionPlanner(
                    spark, datasets.get(relationship.sourceName()), startColumns, endColumns, workers);
        } else {
            planner = new RoundRobinPartitionPlanner(
                    spark, datasets.get(relationship.sourceName()), startColumns, endColumns, workers);
        }

        try {
            for (var wave : planner.plan()) {
                ingestRelationshipWave(planner.getWave(wave), relationship);
            }
        } finally {
            planner.unpersist();
        }
    }

    private void ingestRelationshipWave(DataFrameWriter<Row> waveWriter, RelationshipTargetStep relationship) {
        var sourceLabels = SpecToSparkUtils.getLabels(relationship.startNode().labels());
        var sourceKeys = SpecToSparkUtils.getKeys(relationship.startNode().keyProperties());
        var targetLabels = SpecToSparkUtils.getLabels(relationship.endNode().labels());
        var targetKeys = SpecToSparkUtils.getKeys(relationship.endNode().keyProperties());

        var dfw = applyConfigurationOptions(waveWriter)
                .mode(SpecToSparkUtils.writeMode(relationship.writeMode()))
                .format(FORMAT)
                .option("relationship", relationship.type())
                .option("relationship.source.labels", sourceLabels)
                .option("relationship.source.node.keys", sourceKeys)
                .option("relationship.source.save.mode", SpecToSparkUtils.nodeMatchMode(relationship.nodeMatchMode()))
                .option("relationship.target.labels", targetLabels)
                .option("relationship.target.node.keys", targetKeys)
                .option("relationship.target.save.mode", SpecToSparkUtils.nodeMatchMode(relationship.nodeMatchMode()));

        var relationshipKeys = SpecToSparkUtils.getKeys(relationship.keyProperties());
        var relationshipProperties = SpecToSparkUtils.getProperties(relationship.nonKeyProperties());

        if (!relationshipKeys.isBlank()) {
            dfw = dfw.option("relationship.keys", relationshipKeys);
        }

        if (!relationshipProperties.isBlank()) {
            dfw = dfw.option("relationship.properties", relationshipProperties);
        }

        dfw.save();
    }

    private DataFrameWriter<Row> applyConfigurationOptions(DataFrameWriter<Row> on) {
        for (var opt : this.configuration.keySet()) {
            on = on.option(opt, this.configuration.get(opt));
        }
        return on;
    }
}
