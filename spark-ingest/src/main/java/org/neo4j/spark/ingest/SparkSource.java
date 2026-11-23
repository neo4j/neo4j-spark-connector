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

import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.neo4j.importer.v1.sources.Source;

/**
 * Represents a source of data from Apache Spark. Bring your own data from any Spark format you prefer.
 * Considers the schema and data semantics of the provided Spark dataset when determining equality.
 */
public final class SparkSource implements Source {
    private final String name;
    private final Dataset<Row> data;

    public SparkSource(String name, Dataset<Row> data) {
        this.name = Objects.requireNonNull(name, "name");
        this.data = Objects.requireNonNull(data, "data");
    }

    @Override
    public String getType() {
        return "spark";
    }

    @Override
    public String getName() {
        return name;
    }

    public Dataset<Row> getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SparkSource that)) return false;
        return name.equals(that.name) && data.schema().equals(that.data.schema()) && data.sameSemantics(that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, data.schema(), data.semanticHash());
    }

    @Override
    public String toString() {
        return "SparkSource{" + "name=" + name + ", data=" + data + '}';
    }
}
