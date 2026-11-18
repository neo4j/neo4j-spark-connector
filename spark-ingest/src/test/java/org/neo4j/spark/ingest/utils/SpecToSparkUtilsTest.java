package org.neo4j.spark.ingest.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.importer.v1.pipeline.NodeTargetStep;
import org.neo4j.importer.v1.pipeline.RelationshipTargetStep;
import org.neo4j.importer.v1.targets.*;

public class SpecToSparkUtilsTest {
    @TestFactory
    Stream<DynamicTest> should_get_labels_when() {
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

    @TestFactory
    Stream<DynamicTest> should_get_keys_when() {
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

    @TestFactory
    Stream<DynamicTest> should_get_node_pattern_when() {
        record TestCase(String name, NodeTargetStep node, String expected) {}

        var cases = List.of(
                new TestCase("null node", null, ""),
                new TestCase("simple node", node(List.of("Person")), "(:Person)"),
                new TestCase("composite labels node", node(List.of("Person", "Employee")), "(:Person:Employee)"));

        return cases.stream()
                .map(test -> dynamicTest(
                        test.name(),
                        () -> assertThat(SpecToSparkUtils.getNodePattern(test.node))
                                .isEqualTo(test.expected())));
    }

    @TestFactory
    Stream<DynamicTest> should_get_relationship_pattern_when() {
        record TestCase(String name, RelationshipTargetStep relationship, String expected) {}

        var cases = List.of(
                new TestCase("null relationship", null, ""),
                new TestCase(
                        "simple relationship",
                        relationship("WORKS_FOR", List.of("Person"), List.of("Company")),
                        "(:Person)-[:WORKS_FOR]->(:Company)"),
                new TestCase(
                        "composite start labels",
                        relationship("WORKS_FOR", List.of("Person", "Employee"), List.of("Company")),
                        "(:Person:Employee)-[:WORKS_FOR]->(:Company)"),
                new TestCase(
                        "composite end labels",
                        relationship("WORKS_FOR", List.of("Person"), List.of("Company", "Employer")),
                        "(:Person)-[:WORKS_FOR]->(:Company:Employer)"));

        return cases.stream()
                .map(test -> dynamicTest(
                        test.name(),
                        () -> assertThat(SpecToSparkUtils.getRelationshipPattern(test.relationship()))
                                .isEqualTo(test.expected())));
    }

    private RelationshipTargetStep relationship(String type, List<String> startLabels, List<String> endLabels) {
        var target = new RelationshipTarget(
                true, "test", "", null, type, WriteMode.CREATE, null, (ObjectNode) null, null, null, null, null);

        return new RelationshipTargetStep(target, node(startLabels), node(endLabels), null);
    }

    private NodeTargetStep node(List<String> labels) {
        var target = new NodeTarget(true, "test", "", null, WriteMode.CREATE, (ObjectNode) null, labels, null, null);

        return new NodeTargetStep(target, null);
    }
}
