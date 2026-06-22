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
package org.neo4j.spark.testsupport

import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.support.ParameterDeclarations
import org.testcontainers.neo4j.Neo4jContainer

import java.util.TimeZone
import java.util.stream

class Neo4jContainerProvider extends ArgumentsProvider {

  override def provideArguments(
    parameters: ParameterDeclarations,
    context: ExtensionContext
  ): stream.Stream[Arguments] = {
    stream.Stream.of(
      Arguments.argumentSet("with Vanilla Neo4j", baseContainer),
      Arguments.argumentSet("with Neo4j+APOC core", baseContainer.withPlugins("apoc")),
      Arguments.argumentSet("with Neo4j+GDS", baseContainer.withPlugins("graph-data-science")),
      Arguments.argumentSet("with Neo4j+GDS+APOC core", baseContainer.withPlugins("graph-data-science", "apoc"))
    )
  }

  private def baseContainer = {
    new Neo4jContainer(TestUtil.neo4jImage())
      .withAdminPassword(Neo4jContainerProvider.ADMIN_PASSWORD)
      .withEnv("NEO4J_db_temporal_timezone", TimeZone.getDefault.getID)
      .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
  }
}

object Neo4jContainerProvider {
  val ADMIN_PASSWORD = "letmein!"
}
