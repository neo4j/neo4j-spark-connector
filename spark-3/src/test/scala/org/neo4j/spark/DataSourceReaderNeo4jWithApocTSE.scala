package org.neo4j.spark

import org.junit.Assert.assertEquals
import org.junit.{Assume, BeforeClass, Test}
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{SessionConfig, Transaction, TransactionWork}

class DataSourceReaderNeo4jWithApocTSE extends SparkConnectorScalaBaseWithApocTSE {

  @Test
  def testMultiDbJoin(): Unit = {
    SparkConnectorScalaSuiteWithApocIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
      CREATE (p1:Person:Customer {name: 'John Doe'}),
       (p2:Person:Customer {name: 'Mark Brown'}),
       (p3:Person:Customer {name: 'Cindy White'})
      """).consume()
        })

    SparkConnectorScalaSuiteWithApocIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
      CREATE (p1:Person:Employee {name: 'Jane Doe'}),
       (p2:Person:Employee {name: 'John Doe'})
      """).consume()
        })

    val df1 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("database", "db1")
      .option("labels", "Person")
      .load()

    val df2 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("database", "db2")
      .option("labels", "Person")
      .load()

    assertEquals(3, df1.count())
    assertEquals(2, df2.count())

    val dfJoin = df1.join(df2, df1("name") === df2("name"))
    assertEquals(1, dfJoin.count())
  }

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
