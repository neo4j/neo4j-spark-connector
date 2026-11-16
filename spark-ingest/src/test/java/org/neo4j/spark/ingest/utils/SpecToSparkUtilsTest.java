package org.neo4j.spark.ingest.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.PropertyType;

public class SpecToSparkUtilsTest {
    @Nested
    class GetLabels {
        @TestFactory
        Stream<DynamicTest> should_format_labels_when() {
            record TestCase(String name, List<String> labels, String expected) {}

            var cases = List.of(
                    new TestCase("no labels", List.of(), ""),
                    new TestCase("single label", List.of("Person"), ":Person"),
                    new TestCase("composite label", List.of("Person", "Employee"), ":Person:Employee"));

            return cases.stream()
                    .map(test -> dynamicTest(
                            test.name(),
                            () -> assertThat(SpecToSparkUtils.getLabels(test.labels()))
                                    .isEqualTo(test.expected())));
        }
    }

    @Nested
    class GetKeys {
        @TestFactory
        Stream<DynamicTest> should_format_keys_when() {
            record TestCase(String name, List<PropertyMapping> keyMappings, String expected) {}

            var cases = List.of(
                    new TestCase("no keys", List.of(), ""),
                    new TestCase(
                            "single key mapping",
                            List.of(new PropertyMapping("person_id", "personId", PropertyType.STRING)),
                            "person_id:personId"),
                    new TestCase(
                            "composite key mapping",
                            List.of(
                                    new PropertyMapping("person_id", "personId", PropertyType.STRING),
                                    new PropertyMapping(
                                            "social_security_number", "socialSecurityNumber", PropertyType.STRING)),
                            "person_id:personId,social_security_number:socialSecurityNumber"));

            return cases.stream()
                    .map(test -> dynamicTest(
                            test.name(),
                            () -> assertThat(SpecToSparkUtils.getKeys(test.keyMappings()))
                                    .isEqualTo(test.expected())));
        }
    }
}
