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
import org.neo4j.spark.util.Neo4jOptions

object CypherPreamble {

  def fullPreamble(neo4j: Neo4j, neo4jOptions: Neo4jOptions): String = {
    val version = versionPreamble(neo4j, neo4jOptions)
    val tuning = tuningPreamble(neo4jOptions)

    if (tuning.isEmpty) {
      s"$version"
    } else {
      s"$tuning\n$version"
    }
  }

  private def versionPreamble(neo4j: Neo4j, neo4jOptions: Neo4jOptions): String = {
    if (neo4jOptions.cypherVersion != null && neo4jOptions.cypherVersion.nonEmpty) {
      return s"CYPHER ${neo4jOptions.cypherVersion} "
    }

    if (canIUse(Cypher.explicitCypher5Selection()).withNeo4j(neo4j)) {
      "CYPHER 5 "
    } else {
      ""
    }
  }

  def tuningPreamble(options: Neo4jOptions): String = {
    val cypher = "CYPHER "

    val clause = options.tuning
      .filter { case (parameter, value) => parameter.nonEmpty && value.nonEmpty }
      .map {
        case (parameter, value) if isValidElseThrow(parameter, value) =>
          s"$parameter=$value"
      }
      .mkString(cypher, " ", "")

    if (clause.equals(cypher))
      ""
    else
      clause
  }

  private def isValidElseThrow(parameter: String, parameterValue: String): Boolean = {
    val regex = "^[a-zA-Z0-9_.-]+$"

    if (!parameter.matches(regex)) {
      throw new IllegalArgumentException(
        "Cypher tuning parameter name must be alphanumeric, underscore or hyphen. Found: " + parameter
      )
    }

    if (!parameterValue.matches(regex)) {
      throw new IllegalArgumentException(
        "Cypher tuning parameter value must be alphanumeric, underscore or hyphen. Found: " + parameterValue
      )
    }

    true
  }
}
