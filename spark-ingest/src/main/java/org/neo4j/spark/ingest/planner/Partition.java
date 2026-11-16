package org.neo4j.spark.ingest.planner;

public record Partition(int sourcePartition, int targetPartition) {
    public int id(int globalWorkerCount) {
        return sourcePartition * globalWorkerCount + targetPartition;
    }
}
