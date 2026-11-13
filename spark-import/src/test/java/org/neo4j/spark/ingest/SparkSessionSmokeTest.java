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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

class SparkSessionSmokeTest {

    @Test
    void runsLocalSparkSession() throws Exception {
        try (SparkSession spark = SparkSession.builder()
                .master("local[1]")
                .appName("spark-import-smoke-test")
                .config("spark.ui.enabled", "false")
                .getOrCreate()) {
            assertEquals(3L, spark.range(3).count());
        }
    }
}
