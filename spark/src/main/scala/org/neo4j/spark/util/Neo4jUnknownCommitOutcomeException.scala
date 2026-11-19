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
package org.neo4j.spark.util

/**
 * Thrown when a batch could not be written because the outcome of its transaction commit is unknown, and the
 * configured `transaction.commit.unknown.outcome` policy does not allow the connector to replay it blindly.
 *
 * The batch may or may not be in the database. The connector deliberately does not guess.
 */
class Neo4jUnknownCommitOutcomeException(message: String, cause: Throwable)
    extends RuntimeException(message, cause)
