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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

public class DiagonalPartitionPlannerTest {
    private final SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .appName("diagonal-planner-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate();

    private static final String testCsv = "from_id,to_id\na,b\nc,d\ne,f\ng,h\ni,j\n";

    @TestFactory
    Stream<DynamicTest> should_pick_diagonals_of_n() {
        record TestCase(int n, List<List<Partition>> plan) {}

        var cases = List.of(
                new TestCase(1, List.of(List.of(new Partition(0, 0)))),
                new TestCase(
                        2,
                        List.of(
                                List.of(new Partition(0, 0), new Partition(1, 1)),
                                List.of(new Partition(1, 0), new Partition(0, 1)))),
                new TestCase(
                        3,
                        List.of(
                                List.of(new Partition(0, 0), new Partition(1, 1), new Partition(2, 2)),
                                List.of(new Partition(1, 0), new Partition(2, 1), new Partition(0, 2)),
                                List.of(new Partition(2, 0), new Partition(0, 1), new Partition(1, 2)))),
                new TestCase(
                        4,
                        List.of(
                                List.of(
                                        new Partition(0, 0),
                                        new Partition(1, 1),
                                        new Partition(2, 2),
                                        new Partition(3, 3)),
                                List.of(
                                        new Partition(1, 0),
                                        new Partition(2, 1),
                                        new Partition(3, 2),
                                        new Partition(0, 3)),
                                List.of(
                                        new Partition(2, 0),
                                        new Partition(3, 1),
                                        new Partition(0, 2),
                                        new Partition(1, 3)),
                                List.of(
                                        new Partition(3, 0),
                                        new Partition(0, 1),
                                        new Partition(1, 2),
                                        new Partition(2, 3)))));

        return cases.stream()
                .map(test -> dynamicTest(Integer.toString(test.n), () -> {
                    DiagonalPartitionPlanner planner = new DiagonalPartitionPlanner(
                            spark,
                            spark.read()
                                    .option("header", "true")
                                    .csv(spark.createDataset(testCsv.lines().toList(), Encoders.STRING())),
                            new Column[] {col("from_id")},
                            new Column[] {col("to_id")},
                            test.n);

                    assertThat(planner.plan()).isEqualTo(test.plan);
                }));
    }
}
