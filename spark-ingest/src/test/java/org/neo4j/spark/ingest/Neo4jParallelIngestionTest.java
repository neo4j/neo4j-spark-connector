package org.neo4j.spark.ingest;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
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
    void fruteria_poc() throws SpecificationException {
        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("spark-ingest-smoke-test")
                .config("spark.ui.enabled", "false")
                .getOrCreate();

        SparkSource customerSource = new SparkSource("customer", spark.read().parquet(customer));
        SparkSource invoiceSource = new SparkSource("invoice", spark.read().parquet(invoice));
        SparkSource skuSource = new SparkSource("sku", spark.read().parquet(sku));
        SparkSource fruitSource = new SparkSource("fruit", spark.read().parquet(fruit));

        String rawSpec = """
            ---
            version: '1'
            targets:
              nodes:
                - source: customer
                  name: customer_nodes
                  write_mode: merge
                  labels: [ Customer ]
                  properties:
                    - source_field: customer_id
                      target_property: customerId
                      target_property_type: string
                    - source_field: full_name
                      target_property: fullName
                      target_property_type: string
                  schema:
                    key_constraints:
                      - name: customer_id_key
                        label: Customer
                        properties: [ customerId ]
                    type_constraints:
                      - name: customer_id_type
                        label: Customer
                        property: customerId
                      - name: customer_full_name_type
                        label: Customer
                        property: fullName

                - source: invoice
                  name: invoice_nodes
                  write_mode: merge
                  labels: [ Invoice ]
                  properties:
                    - source_field: invoice_id
                      target_property: invoiceId
                      target_property_type: string
                  schema:
                    key_constraints:
                      - name: invoice_id_key
                        label: Invoice
                        properties: [ invoiceId ]
                    type_constraints:
                      - name: invoice_id_type
                        label: Invoice
                        property: invoiceId

                - source: sku
                  name: sku_nodes
                  write_mode: merge
                  labels: [ Sku ]
                  properties:
                    - source_field: sku_id
                      target_property: skuId
                      target_property_type: string
                    - source_field: description
                      target_property: description
                      target_property_type: string
                  schema:
                    key_constraints:
                      - name: sku_id_key
                        label: Sku
                        properties: [ skuId ]
                    type_constraints:
                      - name: sku_id_type
                        label: Sku
                        property: skuId
                      - name: sku_description_type
                        label: Sku
                        property: description

                - source: fruit
                  name: fruit_type_nodes
                  write_mode: merge
                  labels: [ "Fruit Type" ]
                  properties:
                    - source_field: fruit_type
                      target_property: type
                      target_property_type: string
                  schema:
                    key_constraints:
                      - name: fruit_type_key
                        label: "Fruit Type"
                        properties: [ type ]
                    type_constraints:
                      - name: fruit_type_type
                        label: "Fruit Type"
                        property: type
            """;

        var options = ImportSpecificationDeserializer.Options.builder()
                .externalSources(Set.of(customerSource, invoiceSource, skuSource, fruitSource))
                .build();

        try (var reader = new StringReader(rawSpec)) {
            var specification = ImportSpecificationDeserializer.deserialize(reader, options);

            assertThat(specification.getSources())
                    .containsExactlyInAnyOrder(customerSource, invoiceSource, skuSource, fruitSource);

            Map<String, String> config = new HashMap<>();
            config.put("url", "neo4j://localhost:7687");
            config.put("username", "neo4j");
            config.put("password", "password");
            config.put("workers", "12");

            var plan = new Neo4jParallelIngestion(spark, specification, config);
            plan.ingest(true);
        }
    }
}
