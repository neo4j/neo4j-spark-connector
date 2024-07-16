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
