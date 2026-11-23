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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.neo4j.importer.v1.validation.SpecificationException;

class Neo4jIngestionAPITest {

    @Test
    void creates_plan_from_raw_spec_and_spark_sources() throws IOException, SpecificationException {
        try (SparkSession spark = SparkSession.builder()
                        .master("local[1]")
                        .appName("spark-ingest-plan-api-test")
                        .config("spark.ui.enabled", "false")
                        .getOrCreate();
                var specFile = getClass().getResourceAsStream("/specs/fruteria.yaml")) {
            Set<SparkSource> sources = Set.of(
                    new SparkSource("customer", spark.emptyDataFrame()),
                    new SparkSource("invoice", spark.emptyDataFrame()),
                    new SparkSource("sku", spark.emptyDataFrame()),
                    new SparkSource("fruit", spark.emptyDataFrame()),
                    new SparkSource("ordered", spark.emptyDataFrame()),
                    new SparkSource("has", spark.emptyDataFrame()),
                    new SparkSource("typed", spark.emptyDataFrame()));
            var spec = new String(specFile.readAllBytes(), StandardCharsets.UTF_8);

            var plan = Neo4jIngestionAPI.plan(spark, sources, spec, Map.of());

            assertThat(plan).isNotNull();
        }
    }
}
