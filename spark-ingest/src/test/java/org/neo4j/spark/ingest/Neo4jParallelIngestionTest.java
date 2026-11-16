package org.neo4j.spark.ingest;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.neo4j.importer.v1.ImportSpecificationDeserializer;
import org.neo4j.importer.v1.validation.SpecificationException;

class Neo4jParallelIngestionTest {
    private final String DATA_DIR = System.getProperty("user.home") + "/data/saved/large/";

    private final String customer = DATA_DIR + "customer.parquet";
    private final String invoice = DATA_DIR + "invoice.parquet";
    private final String sku = DATA_DIR + "sku.parquet";
    private final String fruit = DATA_DIR + "fruit.parquet";

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

        var options = ImportSpecificationDeserializer.Options.builder()
                .externalSources(Set.of(customerSource, invoiceSource, skuSource, fruitSource))
                .build();

        try (var reader = new InputStreamReader(
                Objects.requireNonNull(
                        getClass().getResourceAsStream("/specs/fruteria.yaml"),
                        "Missing test resource: /specs/fruteria.yaml"),
                StandardCharsets.UTF_8)) {

            var specification = ImportSpecificationDeserializer.deserialize(reader, options);

            assertThat(specification.getSources())
                    .containsExactlyInAnyOrder(customerSource, invoiceSource, skuSource, fruitSource);

            Map<String, String> config = new HashMap<>();
            config.put("url", "neo4j://localhost:7687");
            config.put("username", "neo4j");
            config.put("password", "pleasetestme");
            config.put("workers", "12");

            var plan = new Neo4jParallelIngestion(spark, specification, config);
            plan.ingest(true);
        }
    }
}
