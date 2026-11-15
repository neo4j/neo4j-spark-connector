package org.neo4j.spark.ingest;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.pipeline.*;

public class Neo4jParallelIngestion {
    private final SparkSession spark;
    private final ImportSpecification spec;
    private final HashMap<String, Dataset<Row>> datasets = new HashMap<>();

    /**
     * configuration for this poc requires
     * url
     * username
     * password
     */
    private Map<String, String> configuration = new HashMap<>();

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
                .option("labels", ParseUtils.getLabels(node.labels()))
                .option("node.keys", ParseUtils.getKeys(node.keyProperties()))
                .save();
    }

    private void ingestRelationship(RelationshipTargetStep relationship, int workers) {
        SparkSession.setActiveSession(spark);
    }
}
