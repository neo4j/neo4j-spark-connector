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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.function.Executable
import org.neo4j.driver.TransactionContext
import org.neo4j.driver.Value
import org.neo4j.spark.testsupport.RowUtil.getByName
import org.neo4j.spark.testsupport.SparkConnectorScalaBaseTSE
import org.neo4j.spark.testsupport.SparkConnectorScalaSuiteIT
import org.neo4j.spark.testsupport.TestUtil
import org.neo4j.spark.testsupport.Versions

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava
import scala.language.postfixOps
import scala.math.Ordering.Implicits.infixOrderingOps

abstract class Neo4jType(`type`: String)

case class Point3d(`type`: String = "point-3d", srid: Int, x: Double, y: Double, z: Double) extends Neo4jType(`type`)

case class Time(`type`: String = "offset-time", value: String) extends Neo4jType(`type`)

case class LocalTimeValue(`type`: String = "local-time", value: String) extends Neo4jType(`type`)

case class Person(name: String, surname: String, age: Int, livesIn: Point3d)

case class Person_TimeAndLocalTime(name: String, time: Time, localTime: LocalTimeValue)

case class SimplePerson(name: String, surname: String)

class DataSourceWriterTSE extends SparkConnectorScalaBaseTSE {

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("DataSourceWriterTSE")
    .getOrCreate()

  import sparkSession.implicits._

  def writeKeyModeRelationshipWriteDataSet(
    optionModifier: Map[String, String] => Map[String, String] = { m => m }
  ): DataFrame = {
    val musicDf = Seq(
      (12, "John Bonham", "Drums", 2, true),
      (19, "John Mayer", "Guitar", 1, false),
      (32, "John Scofield", "Guitar", 3, true),
      (15, "John Butler", "Guitar", 4, false)
    ).toDF("experience", "name", "instrument", "rating", "hasDiploma")

    val options = Map(
      "url" -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      "relationship" -> "PLAYS",
      "relationship.source.save.mode" -> "Overwrite",
      "relationship.target.save.mode" -> "Overwrite",
      "relationship.source.labels" -> ":Musician",
      "relationship.source.node.keys" -> "name",
      "relationship.target.labels" -> ":Instrument",
      "relationship.target.node.keys" -> "instrument:name"
    )
    val modifiedOptions = optionModifier(options)

    musicDf.repartition(1).write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .options(modifiedOptions)
      .save()

    ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship.nodes.map", "false")
      .option("relationship", "PLAYS")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.target.labels", ":Instrument")
      .load()
  }

  @Test
  def `should write relations with KEYS mode with explicitly listed properties`(): Unit = {
    val resultDf = writeKeyModeRelationshipWriteDataSet({ options =>
      options + ("relationship.properties" -> "experience, rating:avgRating, instrument")
    })

    assertEquals(4, resultDf.count())

    val res = resultDf.orderBy("`source.name`").collectAsList()

    assertEquals("John Bonham", getByName[String](res.get(0), "source.name"))
    assertEquals("Drums", getByName[String](res.get(0), "target.name"))
    assertEquals("Drums", getByName[String](res.get(0), "rel.instrument"))
    assertEquals(12, getByName[Long](res.get(0), "rel.experience"))
    assertEquals(2, getByName[Long](res.get(0), "rel.avgRating"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(0).fieldIndex("rel.hasDiploma")): Executable,
      "relationship should not have hasDiploma field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(0).fieldIndex("rel.rating")): Executable,
      "relationship should not have rating field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(0).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )
    assertEquals("John Butler", getByName[String](res.get(1), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(1), "target.name"))
    assertEquals("Guitar", getByName[String](res.get(1), "rel.instrument"))
    assertEquals(15, getByName[Long](res.get(1), "rel.experience"))
    assertEquals(4, getByName[Long](res.get(1), "rel.avgRating"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(1).fieldIndex("rel.hasDiploma")): Executable,
      "relationship should not have hasDiploma field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(1).fieldIndex("rel.rating")): Executable,
      "relationship should not have rating field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(1).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )
    assertEquals("John Mayer", getByName[String](res.get(2), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(2), "target.name"))
    assertEquals("Guitar", getByName[String](res.get(2), "rel.instrument"))
    assertEquals(19, getByName[Long](res.get(2), "rel.experience"))
    assertEquals(1, getByName[Long](res.get(2), "rel.avgRating"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(2).fieldIndex("rel.hasDiploma")): Executable,
      "relationship should not have hasDiploma field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(2).fieldIndex("rel.rating")): Executable,
      "relationship should not have rating field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(2).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )

    assertEquals("John Scofield", getByName[String](res.get(3), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(3), "target.name"))
    assertEquals("Guitar", getByName[String](res.get(3), "rel.instrument"))
    assertEquals(32, getByName[Long](res.get(3), "rel.experience"))
    assertEquals(3, getByName[Long](res.get(3), "rel.avgRating"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(3).fieldIndex("rel.hasDiploma")): Executable,
      "relationship should not have hasDiploma field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(3).fieldIndex("rel.rating")): Executable,
      "relationship should not have rating field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(3).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )
  }

  @Test
  def `should write relations with KEYS mode with explicitly listed empty properties`(): Unit = {
    val resultDf = writeKeyModeRelationshipWriteDataSet({ options =>
      options + ("relationship.properties" -> "")
    })

    assertEquals(4, resultDf.count())

    val res = resultDf.orderBy("`source.name`").collectAsList()

    assertEquals("John Bonham", getByName[String](res.get(0), "source.name"))
    assertEquals("Drums", getByName[String](res.get(0), "target.name"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(0).fieldIndex("rel.experience")): Executable,
      "relationship should not have experience field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(0).fieldIndex("rel.hasDiploma")): Executable,
      "relationship should not have hasDiploma field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(0).fieldIndex("rel.rating")): Executable,
      "relationship should not have rating field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(0).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(0).fieldIndex("rel.instrument")): Executable,
      "relationship should not have instrument field"
    )

    assertEquals("John Butler", getByName[String](res.get(1), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(1), "target.name"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(1).fieldIndex("rel.experience")): Executable,
      "relationship should not have experience field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(1).fieldIndex("rel.hasDiploma")): Executable,
      "relationship should not have hasDiploma field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(1).fieldIndex("rel.rating")): Executable,
      "relationship should not have rating field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(1).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(1).fieldIndex("rel.instrument")): Executable,
      "relationship should not have instrument field"
    )

    assertEquals("John Mayer", getByName[String](res.get(2), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(2), "target.name"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(2).fieldIndex("rel.experience")): Executable,
      "relationship should not have experience field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(2).fieldIndex("rel.hasDiploma")): Executable,
      "relationship should not have hasDiploma field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(2).fieldIndex("rel.rating")): Executable,
      "relationship should not have rating field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(2).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(2).fieldIndex("rel.instrument")): Executable,
      "relationship should not have instrument field"
    )

    assertEquals("John Scofield", getByName[String](res.get(3), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(3), "target.name"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(3).fieldIndex("rel.experience")): Executable,
      "relationship should not have experience field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(3).fieldIndex("rel.hasDiploma")): Executable,
      "relationship should not have hasDiploma field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(3).fieldIndex("rel.rating")): Executable,
      "relationship should not have rating field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(3).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(3).fieldIndex("rel.instrument")): Executable,
      "relationship should not have instrument field"
    )
  }

  @Test
  def `should write relations with KEYS mode with default properties`(): Unit = {
    val resultDf = writeKeyModeRelationshipWriteDataSet()

    assertEquals(4, resultDf.count())

    val res = resultDf.orderBy("`source.name`").collectAsList()

    assertEquals("John Bonham", getByName[String](res.get(0), "source.name"))
    assertEquals("Drums", getByName[String](res.get(0), "target.name"))
    assertEquals(12, getByName[Long](res.get(0), "rel.experience"))
    assertEquals(true, getByName[Boolean](res.get(0), "rel.hasDiploma"))
    assertEquals(2, getByName[Long](res.get(0), "rel.rating"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(0).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(0).fieldIndex("rel.instrument")): Executable,
      "relationship should not have instrument field"
    )

    assertEquals("John Butler", getByName[String](res.get(1), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(1), "target.name"))
    assertEquals(15, getByName[Long](res.get(1), "rel.experience"))
    assertEquals(false, getByName[Boolean](res.get(1), "rel.hasDiploma"))
    assertEquals(4, getByName[Long](res.get(1), "rel.rating"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(1).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(1).fieldIndex("rel.instrument")): Executable,
      "relationship should not have instrument field"
    )

    assertEquals("John Mayer", getByName[String](res.get(2), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(2), "target.name"))
    assertEquals(19, getByName[Long](res.get(2), "rel.experience"))
    assertEquals(false, getByName[Boolean](res.get(2), "rel.hasDiploma"))
    assertEquals(1, getByName[Long](res.get(2), "rel.rating"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(2).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(2).fieldIndex("rel.instrument")): Executable,
      "relationship should not have instrument field"
    )

    assertEquals("John Scofield", getByName[String](res.get(3), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(3), "target.name"))
    assertEquals(32, getByName[Long](res.get(3), "rel.experience"))
    assertEquals(true, getByName[Boolean](res.get(3), "rel.hasDiploma"))
    assertEquals(3, getByName[Long](res.get(3), "rel.rating"))
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(3).fieldIndex("rel.name")): Executable,
      "relationship should not have name field"
    )
    assertThrows(
      classOf[IllegalArgumentException],
      (() => res.get(3).fieldIndex("rel.instrument")): Executable,
      "relationship should not have instrument field"
    )
  }

  @Test
  def `should read and write relations with node overwrite mode`(): Unit = {
    val fixtureQuery: String =
      s"""CREATE (m:Musician {id: 1, name: "John Bonham"})
         |CREATE (i:Instrument {name: "Drums"})
         |CREATE (m)-[:PLAYS {experience: 10}]->(i)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session()
      .executeWrite((tx: TransactionContext) => tx.run(fixtureQuery).consume())

    val musicDf = Seq(
      (1, 12, "John Henry Bonham", "Drums"),
      (2, 19, "John Mayer", "Guitar"),
      (3, 32, "John Scofield", "Guitar"),
      (4, 15, "John Butler", "Guitar")
    ).toDF("id", "experience", "name", "instrument")

    musicDf.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship.nodes.map", "false")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.target.save.mode", "Overwrite")
      .option("relationship", "PLAYS")
      .option("relationship.properties", "experience")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "id")
      .option("relationship.source.node.properties", "name")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save()

    val df2 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship.nodes.map", "false")
      .option("relationship", "PLAYS")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.target.labels", ":Instrument")
      .load()

    val result = df2.where("`source.id` = 1")
      .collectAsList().get(0)

    assertEquals(12, result.getLong(9))
    assertEquals("John Henry Bonham", result.getString(4))
  }

  @Test
  def `should manage script passing the data to the executors`(): Unit = {
    val ds = Seq(SimplePerson("Andrea", "Santurbano"), SimplePerson("Davide", "Fantuzzi")).toDS()
      .repartition(2)

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option(
        "query",
        "CREATE (n:Person{fullName: event.name + ' ' + event.surname, age: scriptResult[0].age[event.name]})"
      )
      .option("script.1", "CREATE INDEX person_surname FOR (p:Person) ON (p.surname);")
      .option("script.2", "CREATE CONSTRAINT product_name_sku FOR (p:Product) REQUIRE (p.name, p.sku) IS NODE KEY;")
      .option("script.3", "RETURN {Andrea: 36, Davide: 32} AS age;")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:Person)
        |WHERE (p.fullName = 'Andrea Santurbano' AND p.age = 36)
        |OR (p.fullName = 'Davide Fantuzzi' AND p.age = 32)
        |RETURN count(p) AS count
        |""".stripMargin
    )
      .single()
      .get("count")
      .asLong()
    val expected = ds.count()
    assertEquals(expected, records)

    val uniqueFieldName = if (TestUtil.neo4jVersion(SparkConnectorScalaSuiteIT.session()) >= Versions.NEO4J_5)
      "owningConstraint"
    else "uniqueness"
    val (indexCondition, uniqueCondition) =
      if (TestUtil.neo4jVersion(SparkConnectorScalaSuiteIT.session()) >= Versions.NEO4J_5) {
        (s"$uniqueFieldName IS NULL", s"$uniqueFieldName IS NOT NULL")
      } else {
        (s"$uniqueFieldName = 'NONUNIQUE'", s"$uniqueFieldName = 'UNIQUE'")
      }
    val query =
      s"""SHOW INDEXES YIELD labelsOrTypes, properties, $uniqueFieldName
         |WHERE (labelsOrTypes = ['Person'] AND properties = ['surname'] AND $indexCondition)
         |OR (labelsOrTypes = ['Product'] AND properties = ['name', 'sku'] AND $uniqueCondition)
         |RETURN count(*) AS count
         |""".stripMargin
    val constraintCount = SparkConnectorScalaSuiteIT.session()
      .run(query)
      .single()
      .get("count")
      .asLong()
    assertEquals(2, constraintCount)
    SparkConnectorScalaSuiteIT.session().run("DROP INDEX person_surname")
    SparkConnectorScalaSuiteIT.session().run("DROP CONSTRAINT product_name_sku")
  }

  @Test
  def `should work create source node and match target node`(): Unit = {
    val data = Seq(
      (12, "John Bonham", "Drums"),
      (19, "John Mayer", "Guitar"),
      (32, "John Scofield", "Guitar"),
      (15, "John Butler", "Guitar")
    )
    SparkConnectorScalaSuiteIT.session().run("CREATE " + data
      .map(_._3)
      .toSet[String]
      .map(instrument => s"(:Instrument{name: '$instrument'})")
      .mkString(", "))
    val musicDf = data.toDF("experience", "name", "instrument")

    musicDf.write
      .mode(SaveMode.Overwrite)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "name")
      .option("relationship.target.save.mode", "match")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save()

    val count = SparkConnectorScalaSuiteIT.session().run(
      """MATCH p = (:Musician)-[:PLAYS]->(:Instrument)
        |RETURN count(p) AS count""".stripMargin
    )
      .single()
      .get("count")
      .asLong()

    assertEquals(data.size, count)
  }

  @Test
  def `should work match source node and merge target node`(): Unit = {
    SparkConnectorScalaSuiteIT.session().run(
      "CREATE CONSTRAINT musician_name FOR (m:Musician) REQUIRE (m.name) IS UNIQUE"
    )
    val data = Seq(
      (12, "John Bonham", "Drums"),
      (19, "John Mayer", "Guitar"),
      (32, "John Scofield", "Guitar"),
      (15, "John Butler", "Guitar")
    )
    SparkConnectorScalaSuiteIT.session().run("CREATE " + data
      .map(_._2)
      .toSet[String]
      .map(name => s"(:Musician{name: '$name'})")
      .mkString(", "))
    val musicDf = data.toDF("experience", "name", "instrument")

    musicDf.repartition(1).write
      .mode(SaveMode.Overwrite)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.source.save.mode", "match")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "name")
      .option("relationship.target.save.mode", "overwrite")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save()

    val count = SparkConnectorScalaSuiteIT.session().run(
      """MATCH p = (:Musician)-[:PLAYS]->(:Instrument)
        |RETURN count(p) AS count""".stripMargin
    )
      .single()
      .get("count")
      .asLong()

    assertEquals(data.size, count)

    SparkConnectorScalaSuiteIT.session().run("DROP CONSTRAINT musician_name")
  }

  @Test
  def `should work match source node and merge target node with odd chars`(): Unit = {
    val data = Seq(
      (12, "John Bonham", "Drums"),
      (19, "John Mayer", "Guitar"),
      (32, "John Scofield", "Guitar"),
      (15, "John Butler", "Guitar")
    )
    val musicDf = data.toDF("experience", "who:name", "instrument")

    musicDf.repartition(1).write
      .mode(SaveMode.Overwrite)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.source.save.mode", "overwrite")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "`who:name`")
      .option("relationship.target.save.mode", "overwrite")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save()

    val count = SparkConnectorScalaSuiteIT.session().run(
      """MATCH p = (:Musician)-[:PLAYS]->(:Instrument)
        |RETURN count(p) AS count""".stripMargin
    )
      .single()
      .get("count")
      .asLong()

    assertEquals(data.size, count)
  }

  @Test
  def shouldWriteComplexDF(): Unit = {
    val data = Seq(
      (
        "Cuba Gooding Jr.",
        1,
        "2022-06-07 00:00:00",
        Seq(Map("product_id" -> 1, "quantity" -> 2), Map("product_id" -> 2, "quantity" -> 4))
      ),
      (
        "Tom Hanks",
        2,
        "2022-07-07 00:00:00",
        Seq(Map("product_id" -> 11, "quantity" -> 2), Map("product_id" -> 22, "quantity" -> 4))
      )
    ).toDF("actor_name", "order_id", "order_date", "products")
    data.write
      .mode(SaveMode.Overwrite)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option(
        "query",
        """
          |MERGE (person:Person {name: event.actor_name})
          |CREATE (order:Order {id: event.order_id, date: datetime(replace(event.order_date, ' ', 'T'))})
          |MERGE (person)-[:CREATED]->(order)
          |WITH event, person, order
          |UNWIND event.products AS product_order
          |MERGE (product:Product {id: product_order.product_id})
          |CREATE (order)-[:CONTAINS{quantityOrdered: product_order.quantity}]->(product)
          |""".stripMargin
      )
      .save()

    val actual = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (p:Person)-[cr:CREATED]->(o:Order)-[co:CONTAINS]->(pr:Product)
        |WITH p, pr, o, co
        |ORDER BY p.name, pr.id
        |RETURN p.name AS name, o.id AS order, collect({id: pr.id, quantity: co.quantityOrdered}) AS products
        |""".stripMargin
    )
      .list()
      .asScala
      .map(_.asMap())
      .toSet
      .asJava
    val expected = Set(
      Map(
        "name" -> "Cuba Gooding Jr.",
        "order" -> 1L,
        "products" -> List(
          Map("id" -> 1L, "quantity" -> 2L).asJava,
          Map("id" -> 2L, "quantity" -> 4L).asJava
        ).asJava
      ).asJava,
      Map(
        "name" -> "Tom Hanks",
        "order" -> 2L,
        "products" -> List(
          Map("id" -> 11L, "quantity" -> 2L).asJava,
          Map("id" -> 22L, "quantity" -> 4L).asJava
        ).asJava
      ).asJava
    ).asJava
    assertEquals(expected, actual)
  }

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
