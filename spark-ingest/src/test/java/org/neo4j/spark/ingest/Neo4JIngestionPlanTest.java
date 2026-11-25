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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.neo4j.importer.v1.ImportSpecificationDeserializer;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.validation.SpecificationException;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.neo4j.Neo4jContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class Neo4JIngestionPlanTest {
    private static final String NEO4J_TEST_IMAGE =
            System.getenv().getOrDefault("NEO4J_TEST_IMAGE", "neo4j:5-enterprise");

    @Container
    private static final Neo4jContainer NEO4J = new Neo4jContainer(
                    DockerImageName.parse(NEO4J_TEST_IMAGE).asCompatibleSubstituteFor("neo4j"))
            .withAdminPassword("password")
            .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes");

    private static final SparkSession SPARK = SparkSession.builder()
            .master("local[*]")
            .appName("spark-ingest-smoke-test")
            .config("spark.ui.enabled", "false")
            .getOrCreate();

    @AfterAll
    public static void tearDown() throws IOException {
        NEO4J.close();
        SPARK.close();
    }

    @Test
    void should_ingest_data() throws SpecificationException, IOException {
        var customerSource = new SparkSource("customer", sparkCsvRead("customer"));
        var invoiceSource = new SparkSource("invoice", sparkCsvRead("invoice"));
        var skuSource = new SparkSource("sku", sparkCsvRead("sku"));
        var fruitSource = new SparkSource("fruit", sparkCsvRead("fruit"));
        var orderedSource = new SparkSource("ordered", sparkCsvRead("customer_ordered_invoice"));
        var hasSource = new SparkSource("has", sparkCsvRead("invoice_contains_sku"));
        var typedSource = new SparkSource("typed", sparkCsvRead("sku_of_type_fruit"));

        Set<Source> externalSources =
                Set.of(customerSource, invoiceSource, skuSource, fruitSource, orderedSource, hasSource, typedSource);

        var optionsWithExternalSources = ImportSpecificationDeserializer.Options.builder()
                .externalSources(externalSources)
                .build();

        Map<String, String> connectorOptions = new HashMap<>();
        connectorOptions.put("workers", "4");
        connectorOptions.put("url", NEO4J.getBoltUrl());
        connectorOptions.put("authentication.type", "basic");
        connectorOptions.put("authentication.basic.username", "neo4j");
        connectorOptions.put("authentication.basic.password", NEO4J.getAdminPassword());
        connectorOptions.put("batch.size", "15"); // creates tons of deadlock opportunities (there won't be any though)

        try (var importSpecFile = new InputStreamReader(
                Objects.requireNonNull(getClass().getResourceAsStream("/specs/fruteria.yaml")),
                StandardCharsets.UTF_8)) {

            var spec = ImportSpecificationDeserializer.deserialize(importSpecFile, optionsWithExternalSources);

            var plan = new Neo4jIngestionPlan(SPARK, spec, connectorOptions);
            plan.ingest(false);
        }

        assertThat(countNodeOfLabel("Customer", connectorOptions)).isEqualTo(500);
        assertThat(countNodeOfLabel("Invoice", connectorOptions)).isEqualTo(1500);
        assertThat(countNodeOfLabel("Sku", connectorOptions)).isEqualTo(100);
        assertThat(countNodeOfLabel("`Fruit Type`", connectorOptions)).isEqualTo(22);

        assertThat(countRelType("ORDERED", connectorOptions)).isEqualTo(1500);
        assertThat(countRelType("ORDERED_BY", connectorOptions)).isEqualTo(1500);
        assertThat(countRelType("CONTAINS", connectorOptions)).isEqualTo(15094);
        assertThat(countRelType("OF_TYPE", connectorOptions)).isEqualTo(100);

        assertThat(countSumOfAmount(connectorOptions)).isEqualTo(30099);
    }

    private long countNodeOfLabel(String label, Map<String, String> opts) {
        return Neo4JIngestionPlanTest.SPARK
                .read()
                .format("neo4j")
                .option("url", opts.get("url"))
                .option("authentication.type", opts.get("authentication.type"))
                .option("authentication.basic.username", opts.get("authentication.basic.username"))
                .option("authentication.basic.password", opts.get("authentication.basic.password"))
                .option("query", "MATCH (n:" + label + ") RETURN count(n) AS count")
                .load()
                .first()
                .getLong(0);
    }

    private long countRelType(String type, Map<String, String> opts) {
        return Neo4JIngestionPlanTest.SPARK
                .read()
                .format("neo4j")
                .option("url", opts.get("url"))
                .option("authentication.type", opts.get("authentication.type"))
                .option("authentication.basic.username", opts.get("authentication.basic.username"))
                .option("authentication.basic.password", opts.get("authentication.basic.password"))
                .option("query", "MATCH ()-[r:" + type + "]->() RETURN count(r) AS count")
                .load()
                .first()
                .getLong(0);
    }

    private long countSumOfAmount(Map<String, String> opts) {
        return Neo4JIngestionPlanTest.SPARK
            .read()
            .format("neo4j")
            .option("url", opts.get("url"))
            .option("authentication.type", opts.get("authentication.type"))
            .option("authentication.basic.username", opts.get("authentication.basic.username"))
            .option("authentication.basic.password", opts.get("authentication.basic.password"))
            .option("query", "MATCH ()-[r:CONTAINS]->() RETURN sum(toInteger(r.amount)) AS totalGoods")
            .option("cypher.tuning.runtime", "parallel")
            .load()
            .first()
            .getLong(0);
    }

    private Dataset<Row> sparkCsvRead(String fileName) {
        var path = Objects.requireNonNull(getClass().getResource("/data/" + fileName + ".csv"))
                .getPath();
        return SPARK.read().option("header", true).csv(path);
    }
}
