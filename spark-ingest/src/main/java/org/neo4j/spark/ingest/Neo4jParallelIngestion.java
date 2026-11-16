package org.neo4j.spark.ingest;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.*;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.pipeline.*;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.spark.ingest.planner.DiagonalPartitionPlanner;
import org.neo4j.spark.ingest.utils.SpecToSparkUtils;

public class Neo4jParallelIngestion {
    private final SparkSession spark;
    private final ImportSpecification spec;
    private final HashMap<String, Dataset<Row>> datasets = new HashMap<>();
    private final Map<String, String> configuration;

    public Neo4jParallelIngestion(SparkSession spark, ImportSpecification spec, Map<String, String> configuration) {
        this.spark = spark;
        this.spec = spec;
        this.configuration = configuration;
    }

    public void ingest(boolean isDisjoint) {
        ingest(isDisjoint, ImportPipeline.of(spec).executionPlan());
    }

    private void ingest(boolean isDisjoint, ImportExecutionPlan plan) {
        if (!isDisjoint) {
            throw new UnsupportedOperationException("non-disjoint ingest is not supported yet");
        }

        for (var group : plan.getGroups()) {
            for (var stage : group.getStages()) {
                for (var step : stage.getSteps()) {
                    if (step instanceof SourceStep sourceStep) {
                        indexSource(sourceStep);
                    } else if (step instanceof NodeTargetStep node) {
                        ingestNode(node, Integer.parseInt(configuration.get("workers")));
                    } else if (step instanceof RelationshipTargetStep relationship) {
                        ingestRelationship(relationship, Integer.parseInt(configuration.get("workers")));
                    } else {
                        throw new IllegalArgumentException("Unsupported step: " + step);
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

    private void ingestNode(NodeTargetStep node, int workers) {
        SparkSession.setActiveSession(spark);

        datasets.get(node.sourceName())
                .repartition(workers)
                .write()
                .mode("Append")
                .format("neo4j")
                .option("url", configuration.get("url"))
                .option("authentication.basic.username", configuration.get("username"))
                .option("authentication.basic.password", configuration.get("password"))
                .option("database", "neo4j")
                .option("batch.size", 20000)
                .option("labels", SpecToSparkUtils.getLabels(node.labels()))
                .option("node.keys", SpecToSparkUtils.getKeys(node.keyProperties()))
                .save();
    }

    private void ingestRelationship(RelationshipTargetStep relationship, int workers) {
        SparkSession.setActiveSession(spark);

        // note: these are arrays because we assume composite keys
        var startColumns = relationship.startNode().keyProperties().stream()
                .map(PropertyMapping::getSourceField)
                .map(functions::col)
                .toArray(Column[]::new);

        var endColumns = relationship.endNode().keyProperties().stream()
                .map(PropertyMapping::getSourceField)
                .map(functions::col)
                .toArray(Column[]::new);

        var planner = new DiagonalPartitionPlanner(
                spark, datasets.get(relationship.sourceName()), startColumns, endColumns, workers);

        for (var wave : planner.plan()) {
            ingestRelationshipWave(planner.getWave(wave), relationship);
        }
    }

    private void ingestRelationshipWave(DataFrameWriter<Row> waveWriter, RelationshipTargetStep relationship) {
        waveWriter
                .mode("Append")
                .format(Constants.FORMAT)
                .option("url", configuration.get("url"))
                .option("authentication.basic.username", configuration.get("username"))
                .option("authentication.basic.password", configuration.get("password"))
                .option("database", "neo4j")
                .option("batch.size", 20000)
                .option("relationship", relationship.type())
                .option(
                        "relationship.source.labels",
                        SpecToSparkUtils.getLabels(relationship.startNode().labels()))
                .option(
                        "relationship.source.node.keys",
                        SpecToSparkUtils.getKeys(relationship.startNode().keyProperties()))
                .option("relationship.source.save.mode", "Match")
                .option(
                        "relationship.target.labels",
                        SpecToSparkUtils.getLabels(relationship.endNode().labels()))
                .option(
                        "relationship.target.node.keys",
                        SpecToSparkUtils.getKeys(relationship.endNode().keyProperties()))
                .option("relationship.target.save.mode", "Match")
                .save();
    }
}
