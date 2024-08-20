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

import org.apache.spark.sql.SparkSession
import org.junit
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test
import org.neo4j.spark.SparkConnectorScalaBaseTSE

class ValidationsTest extends SparkConnectorScalaBaseTSE {

  @Test
  def testVersionThrowsExceptionSparkVersionIsNotSupported(): Unit = {
    val sparkVersion = SparkSession.getActiveSession
      .map { _.version }
      .getOrElse("UNKNOWN")
    try {
      Validations.validate(ValidateSparkMinVersion("99.99"))
      fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}")
    } catch {
      case e: IllegalArgumentException =>
        assertEquals(
          s"""Your current Spark version $sparkVersion is not supported by the current connector.
             |Please visit https://neo4j.com/developer/spark/overview/#_spark_compatibility to know which connector version you need.
             |""".stripMargin,
          e.getMessage
        )
      case e: Throwable =>
        fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}, got ${e.getClass} instead")
    }
  }

  @Test
  def testVersionShouldBeValid(): Unit = {
    val fullVersion = SparkSession
      .getDefaultSession
      .map(_.version)
      .getOrElse("3.2")
    val baseVersion = fullVersion
      .split("\\.")
      .take(2)
      .mkString(".")
    Validations.validate(ValidateSparkMinVersion(s"$baseVersion.*"))
    Validations.validate(ValidateSparkMinVersion(fullVersion))
    Validations.validate(ValidateSparkMinVersion(s"$fullVersion-amzn-0"))
  }

  @Test
  def testVersionShouldValidateTheVersion(): Unit = {
    val version = ValidateSparkMinVersion("2.3.0")
    junit.Assert.assertTrue(version.isSupported("2.3.0-amzn-1"))
    junit.Assert.assertTrue(version.isSupported("2.3.1-amzn-1"))
    junit.Assert.assertTrue(version.isSupported("3.3.0-amzn-1"))
    junit.Assert.assertTrue(version.isSupported("3.3.0"))
    junit.Assert.assertTrue(version.isSupported("3.1.0"))
    junit.Assert.assertTrue(version.isSupported("3.2.0"))
    junit.Assert.assertFalse(version.isSupported("2.2.10"))
  }

}
