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
import org.junit.jupiter.api.extension.ExtensionContext.Namespace
import org.junit.jupiter.api.extension.ExtensionContext.StoreScope
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.ArgumentsProvider
import org.junit.jupiter.params.support.ParameterDeclarations
import org.testcontainers.neo4j.Neo4jContainer

import java.util.TimeZone
import java.util.function.Function
import java.util.stream

class Neo4jContainerProvider extends ArgumentsProvider {

  override def provideArguments(
    parameters: ParameterDeclarations,
    context: ExtensionContext
  ): stream.Stream[Arguments] =
    Neo4jContainerProvider.arguments(context)
}

object Neo4jContainerProvider {
  val ADMIN_PASSWORD = "letmein!"

  private val NAMESPACE = Namespace.create(classOf[Neo4jContainerProvider])
  private val CONTAINERS_KEY = "containers"

  private def arguments(context: ExtensionContext): stream.Stream[Arguments] = {
    val containers = context
      .getStore(StoreScope.LAUNCHER_SESSION, NAMESPACE)
      .computeIfAbsent(
        CONTAINERS_KEY,
        (_: String) => new SharedNeo4jContainers,
        classOf[SharedNeo4jContainers]
      )

    stream.Stream.of(
      Arguments.argumentSet("with Vanilla Neo4j", containers.vanilla),
      Arguments.argumentSet("with Neo4j+APOC core", containers.apoc),
      Arguments.argumentSet("with Neo4j+GDS", containers.gds),
      Arguments.argumentSet("with Neo4j+GDS+APOC core", containers.gdsAndApoc)
    )
  }

  private class SharedNeo4jContainers extends AutoCloseable {

    val vanilla: Neo4jContainer = baseContainer()
    val apoc: Neo4jContainer = baseContainer().withPlugins("apoc")
    val gds: Neo4jContainer = baseContainer().withPlugins("graph-data-science")
    val gdsAndApoc: Neo4jContainer = baseContainer().withPlugins("graph-data-science", "apoc")

    override def close(): Unit = {
      Neo4jExtensions.stopSparkSession()
      Seq(vanilla, apoc, gds, gdsAndApoc).foreach(_.close())
    }

    private def baseContainer() = {
      new Neo4jContainer(TestUtil.neo4jImage())
        .withAdminPassword(ADMIN_PASSWORD)
        .withEnv("NEO4J_db_temporal_timezone", TimeZone.getDefault.getID)
        .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
        .withReuse(true)
    }
  }
}
