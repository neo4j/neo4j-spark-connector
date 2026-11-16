package org.neo4j.spark.ingest.planner;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.xxhash64;

import java.util.List;
import org.apache.spark.sql.*;
import org.neo4j.spark.ingest.Constants;

/**
 * Hashes the start and end nodes of a relationship row in the df that holds the relationship. Then under a modulo
 * operation we partition them into buckets. Ultimately creates relationship buckets which can be picked by a
 * planner to schedule the ingestion of relationships in a lock-free manner.
 */
public abstract class PartitionPlanner {
    protected int partitionCount;
    protected SparkSession spark;
    protected Dataset<Row> hashedDataset;

    public PartitionPlanner(
            SparkSession spark, Dataset<Row> source, Column[] startColumns, Column[] endColumns, int workers) {
        SparkSession.setActiveSession(spark);
        this.partitionCount = workers;
        this.spark = spark;
        this.hashedDataset = source.withColumn(Constants.SOURCE_PARTITION, pmod(xxhash64(startColumns), lit(workers)))
                .withColumn(Constants.TARGET_PARTITION, pmod(xxhash64(endColumns), lit(workers)))
                .withColumn(
                        Constants.RELATIONSHIP_PARTITION,
                        col(Constants.SOURCE_PARTITION).multiply(workers).plus(col(Constants.TARGET_PARTITION)))
                .repartition(workers * workers, col(Constants.RELATIONSHIP_PARTITION))
                .persist();
    }

    public DataFrameWriter<Row> getWave(List<Partition> wave) {
        var partitionIdsOfWave =
                wave.stream().map(partition -> partition.id(partitionCount)).toArray();

        return hashedDataset
                .filter(col(Constants.RELATIONSHIP_PARTITION).isin(partitionIdsOfWave))
                .drop(Constants.SOURCE_PARTITION)
                .drop(Constants.TARGET_PARTITION)
                .drop(Constants.RELATIONSHIP_PARTITION)
                .write();
    }

    /**
     * Generates a list of waves that can each be executed in parallel to each other. A wave is just a list of eligible
     * partitions. Where partitions are represented in hashedDataset. So this plan just creates a logical ordering, no
     * heavy work has been done yet.
     */
    abstract List<List<Partition>> plan();
}
