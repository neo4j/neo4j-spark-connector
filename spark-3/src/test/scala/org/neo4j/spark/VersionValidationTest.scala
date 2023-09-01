package org.neo4j.spark

import org.apache.spark.sql.SparkSession
import org.junit
import org.junit.Assert.{assertEquals, fail}
import org.junit.Test
import org.neo4j.spark.util.{ValidateSparkMinVersion, Validations}

class VersionValidationTest extends SparkConnectorScalaBaseTSE {

  @Test
  def testThrowsExceptionSparkVersionIsNotSupported(): Unit = {
    val sparkVersion = SparkSession.getActiveSession
      .map { _.version }
      .getOrElse("UNKNOWN")
    try {
      Validations.validate(ValidateSparkMinVersion("3.10000"))
      fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}")
    } catch {
      case e: IllegalArgumentException =>
        assertEquals(
          s"""Your current Spark version $sparkVersion is not supported by the current connector.
            |Please visit https://neo4j.com/developer/spark/overview/#_spark_compatibility to know which connector version you need.
            |""".stripMargin, e.getMessage)
      case e: Throwable => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}, got ${e.getClass} instead")
    }
  }


  @Test
  def testShouldBeValid(): Unit = {
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
  def testShouldValidateTheVersion(): Unit = {
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
