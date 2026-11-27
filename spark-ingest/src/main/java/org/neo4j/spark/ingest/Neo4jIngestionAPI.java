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

import java.io.StringReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.neo4j.importer.v1.ImportSpecificationDeserializer;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.validation.SpecificationException;

public final class Neo4jIngestionAPI {

    private Neo4jIngestionAPI() {}

    /**
     * Parses and validates an import specification against externally supplied Spark sources. Then creates a new
     * ingestion plan that can be executed.
     * <p>
     * This method only creates the logical plan for ingestion. No Spark jobs are started until
     * {@link Neo4jIngestionPlan#ingest()} is called.
     * <p>
     * This API uses Neo4j Spark Connector under the hood. You can pass global options to configure all instances of
     * the connector that the ingestion plan will use. It is recommended and appropriate to configure any connection
     * parameters that are required for the ingestion. Such as `url`, `authentication`, `database` and `batch.size`.
     * <p>
     * Note that the options are not limited or validated. What you pass will be bypassed to the connector itself. So
     * use with caution. E.g. do not pass node-only options or relationship-only options (unless your ingestion contains
     * only nodes or only relationships respectively). Do not pass invalid options (keys or values). For more guidance
     * on what options are available, see the documentation of the Neo4j Spark Connector.
     * @see <a href="https://neo4j.com/docs/spark/current/">Neo4j Spark Connector documentation</a>
     *
     * @param spark the Spark session that owns the sources and runs the ingestion
     * @param sources sources referenced by name from the import specification
     * @param spec the raw YAML or JSON import specification
     * @param configuration Neo4j Spark Connector options that will be passed to the connector itself, place your authentication options here.
     * @return an executable ingestion plan
     * @throws SpecificationException if the specification cannot be parsed or fails validation
     */
    public static Neo4jIngestionPlan plan(
            SparkSession spark, Set<SparkSource> sources, String spec, Map<String, String> configuration)
            throws SpecificationException {
        Objects.requireNonNull(spark, "spark");
        Objects.requireNonNull(sources, "sources");
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(configuration, "configuration");

        Set<Source> externalSources = new HashSet<>(sources);

        var options = ImportSpecificationDeserializer.Options.builder()
                .externalSources(externalSources)
                .build();

        var importSpecification = ImportSpecificationDeserializer.deserialize(new StringReader(spec), options);

        return new Neo4jIngestionPlan(spark, importSpecification, configuration);
    }
}
