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

import org.neo4j.cypherdsl.core.Cypher
import org.neo4j.cypherdsl.core.Cypher.callRawCypher
import org.neo4j.cypherdsl.core.Expression
import org.neo4j.cypherdsl.core.ResultStatement
import org.neo4j.cypherdsl.core.StatementBuilder

class QueryEmbedder {

  private val aliaser = new AutomaticAliaser()

  /**
   * This embeds the provided query as a CALL subquery
   * @param originalQuery the original query
   * @param scriptResult the script result, if any
   * @return
   */
  def embed(originalQuery: String, scriptResult: String): StatementBuilder.BuildableStatement[ResultStatement] = {
    val query = aliaser.aliasResults(originalQuery) // unaliased results are not allowed in subqueries
    callRawCypher(s"$scriptResult${query.query}")
      .returning(returnItems(query): _*)
  }

  private def returnItems(query: AliasedQuery): Seq[Expression] =
    query.returnedFields.map {
      case "*"  => Cypher.asterisk()
      case name => Cypher.name(name)
    }
}
