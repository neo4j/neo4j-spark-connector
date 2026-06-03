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
package org.neo4j.spark.cypher

import org.neo4j.caniuse.CanIUse.INSTANCE.canIUse
import org.neo4j.caniuse.Cypher.{INSTANCE => Cypher}
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.cypherdsl.core.Statement
import org.neo4j.cypherdsl.core.renderer.Configuration
import org.neo4j.cypherdsl.core.renderer.Dialect
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.spark.cypher.CypherRenderer.Neo4jV5_23
import org.neo4j.spark.util.Neo4jOptions

case class CypherRenderer(private val neo4j: Neo4j, private val neo4jOptions: Neo4jOptions) {

  def render(statement: Statement): String = {
    cached.render(statement)
  }

  private val cached = Renderer.getRenderer(Configuration.newConfig().withDialect(dialect).build())

  private def dialect: Dialect =
    if (neo4jOptions.cypherVersion != null && neo4jOptions.cypherVersion.nonEmpty) {
      dialectFromOptions()
    } else {
      dialectFromVersion()
    }

  private def dialectFromVersion(): Dialect =
    neo4j.getVersion match {
      case version if version.compareTo(Neo4jV5_23) < 0 =>
        Dialect.NEO4J_5 // this assumes server is at least neo4j 5+, as per documented connector support
      case _ if canIUse(Cypher.explicitCypher25Selection()).withNeo4j(neo4j) =>
        Dialect.NEO4J_2025
      case _ =>
        Dialect.NEO4J_5_DEFAULT_CYPHER
    }

  private def dialectFromOptions(): Dialect =
    neo4jOptions.cypherVersion match {
      case "5" =>
        Dialect.NEO4J_5
      case "25" =>
        Dialect.NEO4J_2025
      case _ =>
        dialectFromVersion()
    }
}

object CypherRenderer {
  private val Neo4jV5_23 = new Neo4jVersion(5, 23, 0)
}
