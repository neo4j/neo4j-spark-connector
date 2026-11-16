package org.neo4j.spark.ingest;

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

class Neo4jParallelIngestionTest {
    private final String DATA_DIR = System.getProperty("user.home") + "/data/saved/large/";

    private final String customer = DATA_DIR + "customer.parquet";
    private final String invoice = DATA_DIR + "invoice.parquet";
    private final String sku = DATA_DIR + "sku.parquet";
    private final String fruit = DATA_DIR + "fruit.parquet";

    private final String customerOrderedInvoice = DATA_DIR + "customer_has_invoice.parquet";
    private final String invoiceContainsSku = DATA_DIR + "invoice_has_sku.parquet";
    private final String skuOfTypeFruit = DATA_DIR + "sku_is_fruit.parquet";

    @Test
    void fruteria_poc() throws SpecificationException, IOException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("spark-ingest-smoke-test")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        SparkSource customerSource = new SparkSource("customer", spark.read().parquet(customer));
        SparkSource invoiceSource = new SparkSource("invoice", spark.read().parquet(invoice));
        SparkSource skuSource = new SparkSource("sku", spark.read().parquet(sku));
        SparkSource fruitSource = new SparkSource("fruit", spark.read().parquet(fruit));

        SparkSource orderedSource = new SparkSource("ordered", spark.read().parquet(customerOrderedInvoice));
        SparkSource hasSource = new SparkSource("has", spark.read().parquet(invoiceContainsSku));
        SparkSource typedSource = new SparkSource("typed", spark.read().parquet(skuOfTypeFruit));

        Set<Source> externalSources =
                Set.of(customerSource, invoiceSource, skuSource, fruitSource, orderedSource, hasSource, typedSource);

        var optionsWithExternalSources = ImportSpecificationDeserializer.Options.builder()
                .externalSources(externalSources)
                .build();

        try (var importSpecFile =
                new InputStreamReader(getClass().getResourceAsStream("/specs/fruteria.yaml"), StandardCharsets.UTF_8)) {

            var importSpecification =
                    ImportSpecificationDeserializer.deserialize(importSpecFile, optionsWithExternalSources);

            Map<String, String> ingestionOptions = new HashMap<>();
            ingestionOptions.put("url", "neo4j://localhost:7687");
            ingestionOptions.put("username", "neo4j");
            ingestionOptions.put("password", "password");
            ingestionOptions.put("workers", "12");

            var plan = new Neo4jParallelIngestion(spark, importSpecification, ingestionOptions);
            boolean isDisjoint = true; // need to be determined by user

            plan.ingest(isDisjoint); // starts deadlock-free data movement into neo4j! 🎉
        }
    }
}
