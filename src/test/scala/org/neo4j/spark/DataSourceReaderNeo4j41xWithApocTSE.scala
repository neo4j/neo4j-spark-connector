package org.neo4j.spark

import org.junit.Assert.assertEquals
import org.junit.{Assume, BeforeClass, Test}

object DataSourceReaderNeo4j41xWithApocTSE {
  @BeforeClass
  def checkNeo4jVersion() {
    val neo4jVersion = TestUtil.neo4jVersion()
    Assume.assumeTrue(!neo4jVersion.startsWith("3.5") && !neo4jVersion.startsWith("4.0"))
  }
}

class DataSourceReaderNeo4j41xWithApocTSE extends SparkConnectorScalaBaseWithApocTSE {

  @Test
  def testReturnProcedure(): Unit = {
    val query =
      """RETURN apoc.convert.toSet([1,1,3]) AS foo, 'bar' AS bar
        |""".stripMargin

    val df = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("partitions", 1)
      .option("query", query)
      .load

    assertEquals(Seq("foo", "bar"), df.columns.toSeq) // ordering should be preserved
    assertEquals(1, df.count())
  }

}
