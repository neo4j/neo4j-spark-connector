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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.TestFactory
import org.neo4j.caniuse.Neo4j
import org.neo4j.spark.util.Neo4jOptions

import java.util.stream.Stream

import scala.jdk.CollectionConverters.SeqHasAsJava

private case class TuningTestCase(
  name: String,
  tuningOptions: Map[String, String],
  expectedOutput: String = null
)

private case class VersionTestCase(
  name: String,
  neo4j: Neo4j,
  cypherVersionOption: String = null,
  expectedOutput: String = null
)

class CypherPreambleTest {

  @TestFactory
  def tuning_preambles_are_empty(): Stream[DynamicTest] = {

    val cases = Seq(
      TuningTestCase("no options are set", Map.empty),
      TuningTestCase("incorrect prefix are ignored", Map("cypher.typo.tuning.update.strategy" -> "eager")),
      TuningTestCase("empty values are ignored", Map("cypher.tuning.empty" -> ""))
    )

    cases.map { testCase =>
      dynamicTest(
        "when " + testCase.name,
        () => {
          val options = new java.util.HashMap[String, String]()
          options.put(Neo4jOptions.URL, "stub-url-doesnt-matter-but-is-required")

          testCase.tuningOptions.foreach { case (key, value) =>
            options.put(key, value)
          }

          val neo4jOptions = new Neo4jOptions(options)
          val tuningPreamble = CypherPreamble.tuningPreamble(neo4jOptions)

          assertThat(tuningPreamble).isEmpty()
        }
      )
    }
      .asJava.stream()
  }

  @TestFactory
  def tuning_preambles_are_valid(): Stream[DynamicTest] = {

    val cases = Seq(
      TuningTestCase("a single option is set", Map("cypher.tuning.runtime" -> "parallel"), "CYPHER runtime=parallel"),
      TuningTestCase(
        "a single option is set (case invariant key)",
        Map("cYphEr.tuNIng.ruNTImE" -> "paRalLel"),
        "CYPHER ruNTImE=paRalLel"
      ),
      TuningTestCase(
        "multiple options are set",
        Map("cypher.tuning.runtime" -> "parallel", "cypher.tuning.planner" -> "cost"),
        "CYPHER planner=cost runtime=parallel"
      ),
      TuningTestCase(
        "multiple options should still ignore empty values",
        Map("cypher.tuning.interpretedPipesFallback" -> "whitelisted_plans_only", "cypher.tuning.ignore.me" -> ""),
        "CYPHER interpretedPipesFallback=whitelisted_plans_only"
      ),
      TuningTestCase(
        "single multi-word option is set",
        Map("cypher.tuning.operatorEngine" -> "interpreted"),
        "CYPHER operatorEngine=interpreted"
      ),
      TuningTestCase(
        "multiple multi-word options are set",
        Map("cypher.tuning.operatorEngine" -> "interpreted", "cypher.tuning.customValue" -> "some-value"),
        "CYPHER operatorEngine=interpreted customValue=some-value"
      )
    )

    cases.map { testCase =>
      dynamicTest(
        "when " + testCase.name,
        () => {
          val options = new java.util.HashMap[String, String]()
          options.put(Neo4jOptions.URL, "stub-url-doesnt-matter-but-is-required")

          testCase.tuningOptions.foreach { case (key, value) =>
            options.put(key, value)
          }

          val neo4jOptions = new Neo4jOptions(options)
          val tuningPreamble = CypherPreamble.tuningPreamble(neo4jOptions)

          assertThat(tuningPreamble).isEqualTo(testCase.expectedOutput)
        }
      )
    }
      .asJava.stream()
  }

  @TestFactory
  def tuning_preambles_are_illegal(): Stream[DynamicTest] = {

    val cases = Seq(
      TuningTestCase("key breaks naming rule because of space", Map("cypher.tuning.a key with space" -> "value")),
      TuningTestCase(
        "value breaks value rule because of space",
        Map("cypher.tuning.update.strategy" -> "no spaces please")
      ),
      TuningTestCase(
        "key breaks value rule because of bad char",
        Map("cypher.tuning.custom¤option" -> "no_special_chars")
      ),
      TuningTestCase("value breaks value rule because of bad char", Map("cypher.tuning.planner" -> "wonky£value"))
    )

    cases.map { testCase =>
      dynamicTest(
        "when " + testCase.name,
        () => {
          val options = new java.util.HashMap[String, String]()
          options.put(Neo4jOptions.URL, "stub-url-doesnt-matter-but-is-required")

          testCase.tuningOptions.foreach { case (key, value) =>
            options.put(key, value)
          }

          val neo4jOptions = new Neo4jOptions(options)

          assertThatThrownBy(() => CypherPreamble.tuningPreamble(neo4jOptions)).isExactlyInstanceOf(
            classOf[IllegalArgumentException]
          )
        }
      )
    }
      .asJava.stream()
  }
}
