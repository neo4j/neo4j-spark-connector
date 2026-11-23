package org.neo4j.spark.ingest.planner;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.xxhash64;

import java.util.List;
import org.apache.spark.sql.*;

/**
 * Hashes the start and end nodes of a relationship row in the df that holds the relationship. Then under a modulo
 * operation we partition them into buckets. Ultimately creates relationship buckets which can be picked by a
 * planner to schedule the ingestion of relationships in a lock-free manner.
 */
public abstract class PartitionPlanner {
    protected int partitionCount;
    protected SparkSession spark;
    protected Dataset<Row> hashedDataset;

    public static final String SOURCE_PARTITION = "_source_partition_column";
    public static final String TARGET_PARTITION = "_target_partition_column";
    public static final String RELATIONSHIP_PARTITION = "_relationship_partition_column";

    public PartitionPlanner(
            SparkSession spark, Dataset<Row> source, Column[] startColumns, Column[] endColumns, int workers) {
        SparkSession.setActiveSession(spark);
        this.partitionCount = workers;
        this.spark = spark;
        this.hashedDataset = source.withColumn(SOURCE_PARTITION, pmod(xxhash64(startColumns), lit(workers)))
                .withColumn(TARGET_PARTITION, pmod(xxhash64(endColumns), lit(workers)))
                .withColumn(
                        RELATIONSHIP_PARTITION,
                        col(SOURCE_PARTITION).multiply(workers).plus(col(TARGET_PARTITION)))
                .repartition(workers * workers, col(RELATIONSHIP_PARTITION))
                .persist();
    }

    public DataFrameWriter<Row> getWave(List<Partition> wave) {
        var partitionIdsOfWave =
                wave.stream().map(partition -> partition.id(partitionCount)).toArray();

        return hashedDataset
                .filter(col(RELATIONSHIP_PARTITION).isin(partitionIdsOfWave))
                .drop(SOURCE_PARTITION)
                .drop(TARGET_PARTITION)
                .drop(RELATIONSHIP_PARTITION)
                .write();
    }

    /**
     * Generates a list of waves that can each be executed in parallel to each other. A wave is just a list of eligible
     * partitions. Where partitions are represented in hashedDataset. So this plan just creates a logical ordering, no
     * heavy work has been done yet.
     */
    public abstract List<List<Partition>> plan();
}
