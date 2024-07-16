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
package org.neo4j.spark

import org.junit.Assert.assertEquals
import org.junit.Test
import org.neo4j.driver.Transaction
import org.neo4j.driver.TransactionWork
import org.neo4j.driver.summary.ResultSummary

class DataSourceReaderAggregationTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def testShouldDoSumAggregation(): Unit = {
    val fixtureQuery: String =
      s"""CREATE (pe:Person {id: 1, fullName: 'Person'})-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr:Product {id: 0, name: 'Product ' + 0, price: 1})
         |WITH pe
         |UNWIND range(1, 10) as id
         |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id, price: id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        }
      )

    ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "BOUGHT")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Product")
      .load
      .createTempView("BOUGHT")

    val df = ss.sql(
      """SELECT `source.fullName`, SUM(DISTINCT(`target.price`)) AS distinctTotal, SUM(`target.price`) AS total
        |FROM BOUGHT
        |group by `source.fullName`""".stripMargin
    )

    val rows = df.collect().toList
    assertEquals(1, rows.length)
    val row = rows(0)
    assertEquals("Person", row.getAs[String]("source.fullName"))
    assertEquals(55L, row.getAs[Long]("distinctTotal"))
    assertEquals(56L, row.getAs[Long]("total"))
  }

  @Test
  def testShouldDoMaxMinAggregation(): Unit = {
    val fixtureQuery: String =
      s"""CREATE (pe:Person {id: 1, fullName: 'Person'})
         |WITH pe
         |UNWIND range(1, 10) as id
         |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id, price: id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        }
      )

    ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "BOUGHT")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Product")
      .load
      .createTempView("BOUGHT")

    val df = ss.sql(
      """SELECT `source.fullName`, MAX(`target.price`) AS max, MIN(`target.price`) AS min
        |FROM BOUGHT
        |GROUP BY `source.fullName`""".stripMargin
    )

    val rows = df.collect().toList
    assertEquals(1, rows.length)
    val row = rows(0)
    assertEquals("Person", row.getAs[String]("source.fullName"))
    assertEquals(10L, row.getAs[Long]("max"))
    assertEquals(1L, row.getAs[Long]("min"))
  }

  @Test
  def testShouldDoCountAggregation(): Unit = {
    val fixtureQuery: String =
      s"""CREATE (pe:Person {id: 1, fullName: 'Person'})-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr:Product {id: 1, name: 'Product 1', price: 1})
         |WITH pe
         |UNWIND range(1, 10) as id
         |MERGE (pr:Product {id: id, name: 'Product ' + id, price: id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        }
      )

    ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "BOUGHT")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Product")
      .load
      .createTempView("BOUGHT")

    val df = ss.sql(
      """SELECT `source.fullName`, COUNT(DISTINCT(`target.id`)) AS distinctTotal, COUNT(`target.id`) AS total
        |FROM BOUGHT
        |group by `source.fullName`""".stripMargin
    )

    val rows = df.collect().toList
    assertEquals(1, rows.length)
    val row = rows(0)
    assertEquals("Person", row.getAs[String]("source.fullName"))
    assertEquals(10L, row.getAs[Long]("distinctTotal"))
    assertEquals(11L, row.getAs[Long]("total"))

  }
}
