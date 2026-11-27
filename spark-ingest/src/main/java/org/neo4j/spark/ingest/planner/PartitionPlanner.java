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
package org.neo4j.spark.ingest.planner;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.xxhash64;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
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

    private final String sourcePartition;
    private final String targetPartition;
    private final String relPartition;

    public PartitionPlanner(
            SparkSession spark, Dataset<Row> source, Column[] startColumns, Column[] endColumns, int partitionCount) {
        SparkSession.setActiveSession(spark);
        this.partitionCount = partitionCount;
        this.spark = spark;

        Set<String> usedColumns = new HashSet<>(Arrays.asList(source.columns()));
        this.sourcePartition = uniqueColumnName(SOURCE_PARTITION, usedColumns);
        this.targetPartition = uniqueColumnName(TARGET_PARTITION, usedColumns);
        this.relPartition = uniqueColumnName(RELATIONSHIP_PARTITION, usedColumns);

        this.hashedDataset =
                applyPartitioningHashes(source, startColumns, endColumns).persist();
    }

    private static String uniqueColumnName(String base, Set<String> usedColumns) {
        if (usedColumns.add(base)) {
            return base;
        }

        for (int suffix = 1; suffix <= 999; suffix++) {
            String candidate = base + "_" + suffix;
            if (usedColumns.add(candidate)) {
                return candidate;
            }
        }

        throw new IllegalArgumentException(
                "Unable to allocate a unique internal column name for " + base + " after 999 attempts");
    }

    private Dataset<Row> applyPartitioningHashes(Dataset<Row> source, Column[] startColumns, Column[] endColumns) {
        return source.withColumn(sourcePartition, pmod(xxhash64(startColumns), lit(partitionCount)))
                .withColumn(targetPartition, pmod(xxhash64(endColumns), lit(partitionCount)))
                .withColumn(
                        relPartition,
                        col(sourcePartition).multiply(partitionCount).plus(col(targetPartition)))
                .select(Stream.concat(
                                Stream.of(sourcePartition, targetPartition, relPartition),
                                Arrays.stream(source.columns()))
                        .map(functions::col)
                        .toArray(Column[]::new))
                .repartition(partitionCount * partitionCount, col(relPartition));
    }

    public void unpersist() {
        hashedDataset.unpersist();
    }

    public DataFrameWriter<Row> getWave(List<Partition> wave) {
        var partitionIdsOfWave =
                wave.stream().map(partition -> partition.id(partitionCount)).toArray();

        return hashedDataset
                .filter(col(relPartition).isin(partitionIdsOfWave))
                .drop(sourcePartition)
                .drop(targetPartition)
                .drop(relPartition)
                .write();
    }

    /**
     * Generates a list of waves that can each be executed in parallel to each other. A wave is just a list of eligible
     * partitions. Where partitions are represented in hashedDataset. So this plan just creates a logical ordering, no
     * heavy work has been done yet.
     */
    public abstract List<List<Partition>> plan();
}
