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

import static org.apache.spark.sql.functions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class PartitionPlannerTest {
    private final SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .appName("partition-planner-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate();

    @TestFactory
    Stream<DynamicTest> should_prevent_helper_column_collisions() {
        record TestCase(List<String> sourceColumns, List<String> expectedColumns) {}

        // shorter names just for readability in cases below:
        String sourcePart = PartitionPlanner.SOURCE_PARTITION;
        String targetPart = PartitionPlanner.TARGET_PARTITION;
        String relPart = PartitionPlanner.RELATIONSHIP_PARTITION;

        var cases = List.of(
                new TestCase(
                        List.of("test_case", "without_collision"),
                        List.of(sourcePart, targetPart, relPart, "test_case", "without_collision")),
                new TestCase(
                        List.of(sourcePart, "collision"),
                        List.of(sourcePart + "_1", targetPart, relPart, sourcePart, "collision")),
                new TestCase(
                        List.of(relPart, sourcePart),
                        List.of(sourcePart + "_1", targetPart, relPart + "_1", relPart, sourcePart)),
                new TestCase(
                        List.of(targetPart, targetPart + "_1", targetPart + "_2"),
                        List.of(
                                sourcePart,
                                targetPart + "_3",
                                relPart,
                                targetPart,
                                targetPart + "_1",
                                targetPart + "_2")),
                new TestCase(
                        List.of(sourcePart + "_1", targetPart + "_1", relPart + "_1"),
                        List.of(
                                sourcePart,
                                targetPart,
                                relPart,
                                sourcePart + "_1",
                                targetPart + "_1",
                                relPart + "_1")));

        return cases.stream()
                .map(test -> dynamicTest(String.join(",", test.sourceColumns()), () -> {
                    Dataset<Row> source = spark.range(1).toDF("value");
                    for (String column : test.sourceColumns()) {
                        source = source.withColumn(column, lit("value"));
                    }
                    source = source.drop("value");

                    var planner = new TestPartitionPlanner(
                            spark, source, new Column[] {lit("start")}, new Column[] {lit("end")}, 2);
                    try {
                        assertThat(planner.hashedDataset.columns())
                                .containsExactly(test.expectedColumns().toArray(String[]::new));
                    } finally {
                        planner.unpersist();
                    }
                }));
    }

    private static final class TestPartitionPlanner extends PartitionPlanner {
        private TestPartitionPlanner(
                SparkSession spark, Dataset<Row> source, Column[] startColumns, Column[] endColumns, int workers) {
            super(spark, source, startColumns, endColumns, workers);
        }

        @Override
        public List<List<Partition>> plan() {
            return List.of();
        }
    }
}
