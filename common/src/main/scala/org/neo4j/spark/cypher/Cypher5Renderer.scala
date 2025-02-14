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

import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jVersion
import org.neo4j.cypherdsl.core.Statement
import org.neo4j.cypherdsl.core.renderer.Configuration
import org.neo4j.cypherdsl.core.renderer.Dialect
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.spark.cypher.Cypher5Renderer.Neo4jV5
import org.neo4j.spark.cypher.CypherVersionSelector.selectCypherVersionClause

class Cypher5Renderer(neo4j: Neo4j) extends Renderer {

  private val delegate =
    Renderer.getRenderer(
      Configuration.newConfig()
        .withDialect(
          if (neo4j.getVersion.compareTo(Neo4jV5) < 0) {
            Dialect.DEFAULT
          } else {
            Dialect.NEO4J_5
          }
        )
        .build()
    )

  override def render(statement: Statement): String = {
    val rendered = delegate.render(statement)
    s"${selectCypherVersionClause(neo4j)}$rendered"
  }

}

private object Cypher5Renderer {
  private val Neo4jV5 = new Neo4jVersion(5, 0, 0)
}
