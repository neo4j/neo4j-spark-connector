package org.neo4j.spark

import org.apache.spark.sql.SparkSession
import org.junit.Assert.{assertEquals, fail}
import org.junit.Test
import org.neo4j.spark.util.{ValidateSparkVersion, Validations}

class VersionValidationTest extends SparkConnectorScalaBaseTSE {

  @Test
  def testThrowsExceptionSparkVersionIsNotSupported(): Unit = {
    val sparkVersion = SparkSession.getActiveSession
      .map { _.version }
      .getOrElse("UNKNOWN")
    try {
      Validations.validate(ValidateSparkVersion("2.4"))
    } catch {
      case e: IllegalArgumentException =>
        assertEquals(
          s"""Your currentSpark version $sparkVersion is not supported by the current connector.
            |Please visit https://neo4j.com/developer/spark/overview/#_spark_compatibility to know which connector version you need.
            |""".stripMargin, e.getMessage)
      case e: Throwable => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}, got ${e.getClass} instead")
    }
  }

}
