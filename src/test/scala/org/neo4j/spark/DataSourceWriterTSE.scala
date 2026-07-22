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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.neo4j.driver.Value
import org.neo4j.spark.testsupport.SparkConnectorScalaBaseTSE
import org.neo4j.spark.testsupport.SparkConnectorScalaSuiteIT

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.language.postfixOps

class DataSourceWriterTSE extends SparkConnectorScalaBaseTSE {

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("DataSourceWriterTSE")
    .getOrCreate()

  import sparkSession.implicits._

  @Test
  def shouldFix502(): Unit = {
    val data = Seq(
      ("Foo", 1, Map("inner" -> Map("key" -> "innerValue"))),
      ("Bar", 1, Map("inner" -> Map("key" -> "innerValue1")))
    ).toDF("id", "time", "table")
    data.write
      .mode(SaveMode.Append)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNodeWithMapFlattend")
      .save()
    val count: Long = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (n:MyNodeWithMapFlattend)
        |WHERE (
        | properties(n) = {id: 'Foo', time: 1, `table.inner.key`: 'innerValue'}
        | OR properties(n) = {id: 'Bar', time: 1, `table.inner.key`: 'innerValue1'}
        |)
        |RETURN count(n)
        |""".stripMargin
    )
      .single()
      .get(0)
      .asLong()
    assertEquals(2L, count)
  }

  @Test
  def shouldFix502WithCollisions(): Unit = {
    val data = Seq(
      ("Foo", 1, ListMap("key.inner" -> Map("key" -> "innerValue"), "key" -> Map("inner.key" -> "value"))),
      ("Bar", 1, ListMap("key.inner" -> Map("key" -> "innerValue1"), "key" -> Map("inner.key" -> "value1")))
    ).toDF("id", "time", "table")
    data.write
      .mode(SaveMode.Append)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNodeWithMapFlattend")
      .save()
    val count: Long = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (n:MyNodeWithMapFlattend)
        |WHERE (
        | properties(n) = {id: 'Foo', time: 1, `table.key.inner.key`: 'value'}
        | OR properties(n) = {id: 'Bar', time: 1, `table.key.inner.key`: 'value1'}
        |)
        |RETURN count(n)
        |""".stripMargin
    )
      .single()
      .get(0)
      .asLong()
    assertEquals(2L, count)
  }

  @Test
  def shouldFix502WithCollisionsAndAggregateValues(): Unit = {
    val data = Seq(
      ("Foo", 1, ListMap("key.inner" -> Map("key" -> "innerValue"), "key" -> Map("inner.key" -> "value"))),
      ("Bar", 1, ListMap("key.inner" -> Map("key" -> "innerValue1"), "key" -> Map("inner.key" -> "value1")))
    ).toDF("id", "time", "table")
    data.write
      .mode(SaveMode.Append)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNodeWithMapFlattend")
      .option("schema.map.group.duplicate.keys", true)
      .save()
    val count: Long = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (n:MyNodeWithMapFlattend)
        |WHERE (
        | properties(n) = {id: 'Foo', time: 1, `table.key.inner.key`: ['innerValue', 'value']}
        | OR properties(n) = {id: 'Bar', time: 1, `table.key.inner.key`: ['innerValue1', 'value1']}
        |)
        |RETURN count(n)
        |""".stripMargin
    )
      .single()
      .get(0)
      .asLong()
    assertEquals(2L, count)
  }

  @Test
  def doesNotWriteNodePropertiesToRelationship(): Unit = {
    val data = Seq(
      ("john", "The Matrix", "today"),
      ("jane", "Oppenheimer", "yesterday"),
      ("şaban", "Hababam Sınıfı", "two days ago")
    ).toDF("username", "movie_title", "watch_time")
    data.write
      .mode(SaveMode.Append)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "WATCHED")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.source.labels", ":User")
      .option("relationship.source.node.keys", "username:name")
      .option("relationship.target.save.mode", "Overwrite")
      .option("relationship.target.labels", ":Movie")
      .option("relationship.target.node.keys", "movie_title:title")
      .save()
    val rows = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (:User)-[r:WATCHED]->(:Movie)
        |WITH r
        |ORDER BY r.watch_time ASC
        |RETURN collect(r{.*})
        |""".stripMargin
    )
      .single()
      .get(0)
      .asList((value: Value) => value.asMap().asScala)
      .asScala
    assertEquals(
      List(
        Map("watch_time" -> "today"),
        Map("watch_time" -> "two days ago"),
        Map("watch_time" -> "yesterday")
      ),
      rows
    )
  }
}
