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

object CypherVersionSelector {

  def selectCypherVersionClause(neo4j: Neo4j): String = {
    if (canIUse(Cypher.explicitCypher5Selection()).withNeo4j(neo4j)) {
      "CYPHER 5 "
    } else {
      ""
    }
  }
}
