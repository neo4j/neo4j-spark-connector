package org.neo4j.spark

import org.junit.Assert.assertEquals
import org.junit.Test
import org.neo4j.driver.Transaction
import org.neo4j.driver.TransactionWork
import org.neo4j.driver.summary.ResultSummary

class DefaultConfigTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def `when session has default parameters it should use those instead of requiring options`(): Unit = {

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("CREATE (p:Person {name: 'Foobar'})").consume()
        }
      )

    ss.conf.set("neo4j.url", SparkConnectorScalaSuiteIT.server.getBoltUrl)

    val df = ss.read.format(classOf[DataSource].getName)
      .option("labels", "Person")
      .load()

    assertEquals(df.count(), 1)
  }

}
