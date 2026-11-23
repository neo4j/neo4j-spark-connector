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
import java.util.Set;
import org.apache.spark.sql.SparkSession;
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

    private final String customer = getClass().getResource("/data/customer.csv").getPath();
    private final String invoice = getClass().getResource("/data/invoice.csv").getPath();
    private final String sku = getClass().getResource("/data/sku.csv").getPath();
    private final String fruit = getClass().getResource("/data/fruit.csv").getPath();

    private final String customerOrderedInvoice =
            getClass().getResource("/data/customer_ordered_invoice.csv").getPath();
    private final String invoiceContainsSku =
            getClass().getResource("/data/invoice_contains_sku.csv").getPath();
    private final String skuOfTypeFruit =
            getClass().getResource("/data/sku_of_type_fruit.csv").getPath();

    @Test
    void fruteria_poc() throws SpecificationException, IOException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("spark-ingest-smoke-test")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        SparkSource customerSource = new SparkSource(
                "customer", spark.read().option("header", "true").csv(customer));
        SparkSource invoiceSource =
                new SparkSource("invoice", spark.read().option("header", "true").csv(invoice));
        SparkSource skuSource =
                new SparkSource("sku", spark.read().option("header", "true").csv(sku));
        SparkSource fruitSource =
                new SparkSource("fruit", spark.read().option("header", "true").csv(fruit));

        SparkSource orderedSource =
                new SparkSource("ordered", spark.read().option("header", "true").csv(customerOrderedInvoice));
        SparkSource hasSource =
                new SparkSource("has", spark.read().option("header", "true").csv(invoiceContainsSku));
        SparkSource typedSource =
                new SparkSource("typed", spark.read().option("header", "true").csv(skuOfTypeFruit));

        Set<Source> externalSources =
                Set.of(customerSource, invoiceSource, skuSource, fruitSource, orderedSource, hasSource, typedSource);

        var optionsWithExternalSources = ImportSpecificationDeserializer.Options.builder()
                .externalSources(externalSources)
                .build();

        Map<String, String> ingestionOptions = new HashMap<>();
        ingestionOptions.put("url", NEO4J.getBoltUrl());
        ingestionOptions.put("username", "neo4j");
        ingestionOptions.put("password", "password");
        ingestionOptions.put("workers", "4");
        ingestionOptions.put("batch.size", "15"); // creates tons of deadlock opportunities (there won't be any though)

        try (var importSpecFile =
                new InputStreamReader(getClass().getResourceAsStream("/specs/fruteria.yaml"), StandardCharsets.UTF_8)) {

            var importSpecification =
                    ImportSpecificationDeserializer.deserialize(importSpecFile, optionsWithExternalSources);

            var plan = new Neo4jIngestionPlan(spark, importSpecification, ingestionOptions);
            boolean isDisjoint = true; // need to be determined by user

            plan.ingest(isDisjoint); // starts deadlock-free data movement into neo4j! 🎉
        }

        assertThat(countNodeOfLabel(spark, "Customer", ingestionOptions)).isEqualTo(500);
        assertThat(countNodeOfLabel(spark, "Invoice", ingestionOptions)).isEqualTo(1500);
        assertThat(countNodeOfLabel(spark, "Sku", ingestionOptions)).isEqualTo(100);
        assertThat(countNodeOfLabel(spark, "Fruit Type", ingestionOptions)).isEqualTo(22);

        assertThat(countRelationshipOfType(spark, "ORDERED", ingestionOptions)).isEqualTo(1500);
        assertThat(countRelationshipOfType(spark, "ORDERED_BY", ingestionOptions))
                .isEqualTo(1500);
        assertThat(countRelationshipOfType(spark, "CONTAINS", ingestionOptions)).isEqualTo(15094);
        assertThat(countRelationshipOfType(spark, "OF_TYPE", ingestionOptions)).isEqualTo(100);
    }

    private long countNodeOfLabel(SparkSession spark, String label, Map<String, String> opts) {
        return spark.read()
                .format("neo4j")
                .option("url", opts.get("url"))
                .option("authentication", "basic")
                .option("authentication.basic.username", opts.get("username"))
                .option("authentication.basic.password", opts.get("password"))
                .option("labels", label)
                .load()
                .count();
    }

    private long countRelationshipOfType(SparkSession spark, String type, Map<String, String> opts) {
        return spark.read()
                .format("neo4j")
                .option("url", opts.get("url"))
                .option("authentication", "basic")
                .option("authentication.basic.username", opts.get("username"))
                .option("authentication.basic.password", opts.get("password"))
                .option("query", "MATCH ()-[r:" + type + "]->() RETURN r")
                .load()
                .count();
    }
}
