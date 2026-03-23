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

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit
import org.junit.Assert.*
import org.junit.Ignore
import org.junit.Test
import org.neo4j.driver.TransactionContext
import org.neo4j.driver.Value
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.internal.InternalPoint2D
import org.neo4j.driver.internal.InternalPoint3D
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.types.IsoDuration
import org.neo4j.driver.types.Type
import org.neo4j.spark.RowUtil.getByName
import org.neo4j.spark.util.Neo4jOptions
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.must.Matchers.the
import org.scalatest.matchers.should.Matchers.convertToStringShouldWrapperForVerb

import java.time.LocalTime
import java.time.OffsetTime

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters.*
import scala.language.postfixOps
import scala.math.Ordering.Implicits.infixOrderingOps
import scala.util.Random

abstract class Neo4jType(`type`: String)

case class Duration(`type`: String = "duration", months: Long, days: Long, seconds: Long, nanoseconds: Long)
    extends Neo4jType(`type`)

case class Point2d(`type`: String = "point-2d", srid: Int, x: Double, y: Double) extends Neo4jType(`type`)

case class Point3d(`type`: String = "point-3d", srid: Int, x: Double, y: Double, z: Double) extends Neo4jType(`type`)

case class Time(`type`: String = "offset-time", value: String) extends Neo4jType(`type`)

case class LocalTimeValue(`type`: String = "local-time", value: String) extends Neo4jType(`type`)

case class Person(name: String, surname: String, age: Int, livesIn: Option[Point3d])

case class Person_TimeAndLocalTime(name: String, time: Time, localTime: LocalTimeValue)

case class SimplePerson(name: String, surname: Option[String])

case class EmptyRow[T](data: T)

class DataSourceWriterTSE extends SparkConnectorScalaBaseTSE {

  private def dfSingleFooString(values: Seq[String]): DataFrame = {
    val st = StructType(Seq(StructField("foo", DataTypes.StringType, true)))
    ss.createDataFrame(values.map(s => Row(s)).asJava, st)
  }

  private def dfSingleFooInt(values: Seq[Int]): DataFrame = {
    val st = StructType(Seq(StructField("foo", DataTypes.IntegerType, true)))
    ss.createDataFrame(values.map(i => Row(Int.box(i))).asJava, st)
  }

  private def dfSingleFooDate(values: Seq[java.sql.Date]): DataFrame = {
    val st = StructType(Seq(StructField("foo", DataTypes.DateType, true)))
    ss.createDataFrame(values.map(d => Row(d)).asJava, st)
  }

  private def dfSingleFooTimestamp(values: Seq[java.sql.Timestamp]): DataFrame = {
    val st = StructType(Seq(StructField("foo", DataTypes.TimestampType, true)))
    ss.createDataFrame(values.map(t => Row(t)).asJava, st)
  }

  private def dfSingleFooStringArray(rows: Seq[Array[String]]): DataFrame = {
    val st = StructType(Seq(StructField("foo", DataTypes.createArrayType(DataTypes.StringType, false), true)))
    ss.createDataFrame(rows.map(a => Row(a)).asJava, st)
  }

  private def dfSingleFooLongArray(rows: Seq[Array[Long]]): DataFrame = {
    val st = StructType(Seq(StructField("foo", DataTypes.createArrayType(DataTypes.LongType, false), true)))
    ss.createDataFrame(rows.map(a => Row(a)).asJava, st)
  }

  private def dfSingleFooMapStringInt(rows: Seq[Map[String, Int]]): DataFrame = {
    val mt = DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType)
    val st = StructType(Seq(StructField("foo", mt, true)))
    ss.createDataFrame(
      rows.map(m => Row(m.map { case (k, v) => k -> Int.box(v) }.asJava)).asJava,
      st
    )
  }

  private def dfMusician3(rows: (Int, String, String)*): DataFrame = {
    val st = StructType(
      Seq(
        StructField("experience", DataTypes.IntegerType, false),
        StructField("name", DataTypes.StringType, false),
        StructField("instrument", DataTypes.StringType, false)
      )
    )
    ss.createDataFrame(rows.map { case (a, b, c) => Row(Int.box(a), b, c) }.asJava, st)
  }

  private def dfMusician3WhoName(rows: (Int, String, String)*): DataFrame = {
    val st = StructType(
      Seq(
        StructField("experience", DataTypes.IntegerType, false),
        StructField("who:name", DataTypes.StringType, false),
        StructField("instrument", DataTypes.StringType, false)
      )
    )
    ss.createDataFrame(rows.map { case (a, b, c) => Row(Int.box(a), b, c) }.asJava, st)
  }

  private def dfMusician4Unusual(rows: (Int, String, String, String)*): DataFrame = {
    val st = StructType(
      Seq(
        StructField("experience", DataTypes.IntegerType, false),
        StructField("name", DataTypes.StringType, false),
        StructField("instrument", DataTypes.StringType, false),
        StructField("fi``(╯°□°)╯︵ ┻━┻eld", DataTypes.StringType, false)
      )
    )
    ss.createDataFrame(rows.map { case (a, b, c, d) => Row(Int.box(a), b, c, d) }.asJava, st)
  }

  private def dfMusician4WithId(rows: (Int, Int, String, String)*): DataFrame = {
    val st = StructType(
      Seq(
        StructField("id", DataTypes.IntegerType, false),
        StructField("experience", DataTypes.IntegerType, false),
        StructField("name", DataTypes.StringType, false),
        StructField("instrument", DataTypes.StringType, false)
      )
    )
    ss.createDataFrame(rows.map { case (a, b, c, d) => Row(Int.box(a), Int.box(b), c, d) }.asJava, st)
  }

  private def dfMusician5(rows: (Int, String, String, Int, Boolean)*): DataFrame = {
    val st = StructType(
      Seq(
        StructField("experience", DataTypes.IntegerType, false),
        StructField("name", DataTypes.StringType, false),
        StructField("instrument", DataTypes.StringType, false),
        StructField("rating", DataTypes.IntegerType, false),
        StructField("hasDiploma", DataTypes.BooleanType, false)
      )
    )
    ss.createDataFrame(
      rows.map { case (a, b, c, d, e) =>
        Row(Int.box(a), b, c, Int.box(d), java.lang.Boolean.valueOf(e))
      }.asJava,
      st
    )
  }

  private def dfSurnameStrings(values: Seq[String]): DataFrame = {
    val st = StructType(Seq(StructField("surname", DataTypes.StringType, true)))
    ss.createDataFrame(values.map(s => Row(s)).asJava, st)
  }

  private def dfNameInstrument(rows: (String, String)*): DataFrame = {
    val st = StructType(
      Seq(
        StructField("name", DataTypes.StringType, false),
        StructField("instrument", DataTypes.StringType, false)
      )
    )
    ss.createDataFrame(rows.map { case (a, b) => Row(a, b) }.asJava, st)
  }

  /** Row/schema-based: Kryo on [[SimplePerson]] does not reliably round-trip `Option` fields for the write path. */
  private def dfSimplePerson(seq: Seq[SimplePerson]): DataFrame = {
    val st = StructType(
      Seq(
        StructField("name", DataTypes.StringType, false),
        StructField("surname", DataTypes.StringType, true)
      )
    )
    ss.createDataFrame(
      seq.map { case SimplePerson(n, s) => Row(n, s.orNull) }.asJava,
      st
    )
  }

  private val point2dStruct: StructType = StructType(
    Seq(
      StructField("type", DataTypes.StringType, true),
      StructField("srid", DataTypes.IntegerType, false),
      StructField("x", DataTypes.DoubleType, false),
      StructField("y", DataTypes.DoubleType, false)
    )
  )

  private val point3dStruct: StructType = StructType(
    Seq(
      StructField("type", DataTypes.StringType, true),
      StructField("srid", DataTypes.IntegerType, false),
      StructField("x", DataTypes.DoubleType, false),
      StructField("y", DataTypes.DoubleType, false),
      StructField("z", DataTypes.DoubleType, false)
    )
  )

  private val durationStruct: StructType = StructType(
    Seq(
      StructField("type", DataTypes.StringType, true),
      StructField("months", DataTypes.LongType, false),
      StructField("days", DataTypes.LongType, false),
      StructField("seconds", DataTypes.LongType, false),
      StructField("nanoseconds", DataTypes.LongType, false)
    )
  )

  private val timeValueStruct: StructType = StructType(
    Seq(
      StructField("type", DataTypes.StringType, true),
      StructField("value", DataTypes.StringType, false)
    )
  )

  private def rowPoint2d(p: Point2d): Row =
    Row(p.`type`, Int.box(p.srid), p.x, p.y)

  private def rowPoint3d(p: Point3d): Row =
    Row(p.`type`, Int.box(p.srid), p.x, p.y, p.z)

  private def rowDuration(d: Duration): Row =
    Row(d.`type`, Long.box(d.months), Long.box(d.days), Long.box(d.seconds), Long.box(d.nanoseconds))

  private def rowTime(t: Time): Row = Row(t.`type`, t.value)

  private def rowLocalTimeValue(l: LocalTimeValue): Row = Row(l.`type`, l.value)

  private def tuplePoint2d(s: Row): (Int, Double, Double) =
    (s.getInt(1), s.getDouble(2), s.getDouble(3))

  private def tuplePoint3d3(s: Row): (Int, Double, Double) =
    (s.getInt(1), s.getDouble(2), s.getDouble(3))

  private def tuplePoint3d4(s: Row): (Int, Double, Double, Double) =
    (s.getInt(1), s.getDouble(2), s.getDouble(3), s.getDouble(4))

  private def tupleDuration(s: Row): (Long, Long, Long, Long) =
    (s.getLong(1), s.getLong(2), s.getLong(3), s.getLong(4))

  private def seqRow(cell: Any): Seq[Row] = cell match {
    case null                       => Seq.empty
    case s: scala.collection.Seq[?] => s.toSeq.asInstanceOf[Seq[Row]]
    case l: java.util.List[?]       => l.asScala.toSeq.asInstanceOf[Seq[Row]]
    case a: Array[?]                => (0 until a.length).map(i => a(i).asInstanceOf[Row]).toSeq
  }

  private def dfPerson(seq: Seq[Person]): DataFrame = {
    val st = StructType(
      Seq(
        StructField("name", DataTypes.StringType, false),
        StructField("surname", DataTypes.StringType, false),
        StructField("age", DataTypes.IntegerType, false),
        StructField("livesIn", point3dStruct, true)
      )
    )
    ss.createDataFrame(
      seq.map {
        case Person(n, s, a, None)         => Row(n, s, Int.box(a), null)
        case Person(n, s, a, Some(point3)) => Row(n, s, Int.box(a), rowPoint3d(point3))
      }.asJava,
      st
    )
  }

  private def dfEmptyRowPoint2d(seq: Seq[EmptyRow[Point2d]]): DataFrame = {
    val st = StructType(Seq(StructField("data", point2dStruct, false)))
    ss.createDataFrame(seq.map { case EmptyRow(p) => Row(rowPoint2d(p)) }.asJava, st)
  }

  private def dfEmptyRowSeqPoint2d(seq: Seq[EmptyRow[Seq[Point2d]]]): DataFrame = {
    val arr = DataTypes.createArrayType(point2dStruct, false)
    val st = StructType(Seq(StructField("data", arr, false)))
    ss.createDataFrame(
      seq.map { case EmptyRow(points) => Row(points.map(rowPoint2d).toArray) }.asJava,
      st
    )
  }

  private def dfEmptyRowPoint3d(seq: Seq[EmptyRow[Point3d]]): DataFrame = {
    val st = StructType(Seq(StructField("data", point3dStruct, false)))
    ss.createDataFrame(seq.map { case EmptyRow(p) => Row(rowPoint3d(p)) }.asJava, st)
  }

  private def dfEmptyRowSeqPoint3d(seq: Seq[EmptyRow[Seq[Point3d]]]): DataFrame = {
    val arr = DataTypes.createArrayType(point3dStruct, false)
    val st = StructType(Seq(StructField("data", arr, false)))
    ss.createDataFrame(
      seq.map { case EmptyRow(points) => Row(points.map(rowPoint3d).toArray) }.asJava,
      st
    )
  }

  private def dfEmptyRowDuration(seq: Seq[EmptyRow[Duration]]): DataFrame = {
    val st = StructType(Seq(StructField("data", durationStruct, false)))
    ss.createDataFrame(seq.map { case EmptyRow(d) => Row(rowDuration(d)) }.asJava, st)
  }

  private def dfEmptyRowSeqDuration(seq: Seq[EmptyRow[Seq[Duration]]]): DataFrame = {
    val arr = DataTypes.createArrayType(durationStruct, false)
    val st = StructType(Seq(StructField("data", arr, false)))
    ss.createDataFrame(
      seq.map { case EmptyRow(durations) => Row(durations.map(rowDuration).toArray) }.asJava,
      st
    )
  }

  private def dfPersonTimeLocal(seq: Seq[Person_TimeAndLocalTime]): DataFrame = {
    val st = StructType(
      Seq(
        StructField("name", DataTypes.StringType, false),
        StructField("time", timeValueStruct, false),
        StructField("localTime", timeValueStruct, false)
      )
    )
    ss.createDataFrame(
      seq.map { case Person_TimeAndLocalTime(n, t, lt) => Row(n, rowTime(t), rowLocalTimeValue(lt)) }.asJava,
      st
    )
  }

  private def dfOrderComplex(
    rows: (String, Int, String, Seq[Map[String, Int]])*
  ): DataFrame = {
    val mapType = DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType)
    val productsType = DataTypes.createArrayType(mapType)
    val st = StructType(
      Seq(
        StructField("actor_name", DataTypes.StringType, false),
        StructField("order_id", DataTypes.IntegerType, false),
        StructField("order_date", DataTypes.StringType, false),
        StructField("products", productsType, false)
      )
    )
    val sparkRows = rows.map { case (name, oid, date, products) =>
      Row(
        name,
        Int.box(oid),
        date,
        products.map(m => m.map { case (k, v) => k -> Int.box(v) }.asJava).asJava
      )
    }
    ss.createDataFrame(sparkRows.asJava, st)
  }

  private def dfNestedMapTable(rows: (String, Int, Map[String, Map[String, String]])*): DataFrame = {
    val inner = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)
    val tableType = DataTypes.createMapType(DataTypes.StringType, inner)
    val st = StructType(
      Seq(
        StructField("id", DataTypes.StringType, false),
        StructField("time", DataTypes.IntegerType, false),
        StructField("table", tableType, false)
      )
    )
    ss.createDataFrame(
      rows.map { case (id, time, table) =>
        Row(id, Int.box(time), table.map { case (k, v) => k -> v.asJava }.asJava)
      }.asJava,
      st
    )
  }

  private def dfListMapTable(rows: (String, Int, ListMap[String, Map[String, String]])*): DataFrame = {
    val inner = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)
    val tableType = DataTypes.createMapType(DataTypes.StringType, inner)
    val st = StructType(
      Seq(
        StructField("id", DataTypes.StringType, false),
        StructField("time", DataTypes.IntegerType, false),
        StructField("table", tableType, false)
      )
    )
    ss.createDataFrame(
      rows.map { case (id, time, table) =>
        val jOuter = new java.util.LinkedHashMap[String, java.util.Map[String, String]]()
        table.foreach { case (k, v) => jOuter.put(k, v.asJava) }
        Row(id, Int.box(time), jOuter)
      }.asJava,
      st
    )
  }

  private def dfWatched(rows: (String, String, String)*): DataFrame = {
    val st = StructType(
      Seq(
        StructField("username", DataTypes.StringType, false),
        StructField("movie_title", DataTypes.StringType, false),
        StructField("watch_time", DataTypes.StringType, false)
      )
    )
    ss.createDataFrame(rows.map { case (a, b, c) => Row(a, b, c) }.asJava, st)
  }

  private def testType[T](ds: DataFrame, neo4jType: Type): Unit = {
    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.foo AS foo
        |""".stripMargin
    ).list().asScala
      .filter(r => r.get("foo").hasType(neo4jType))
      .map(r => r.asMap().asScala)
      .toSet
    val expected = ds.collect()
      .map(row =>
        Map("foo" -> {
          val foo = row.getAs[T]("foo")
          foo match {
            case sqlDate: java.sql.Date => sqlDate
                .toLocalDate
            case sqlTimestamp: java.sql.Timestamp => sqlTimestamp.toLocalDateTime
            case _                                => foo
          }
        })
      )
      .toSet
    assertEquals(expected, records)
  }

  private def testArray[T](ds: DataFrame): Unit = {
    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.foo AS foo
        |""".stripMargin
    ).list().asScala
      .filter(r => r.get("foo").hasType(InternalTypeSystem.TYPE_SYSTEM.LIST()))
      .map(r => r.get("foo").asList())
      .toSet
    val expected = ds.collect()
      .map(row => row.getList[T](0))
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def testThrowsExceptionIfNoValidReadOptionIsSet(): Unit = {
    try {
      ss.read.format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .load()
        .show() // we need the action to be able to trigger the exception because of the changes in Spark 3
    } catch {
      case e: IllegalArgumentException =>
        assertEquals("No valid option found. One of `GDS`, `LABELS`, `QUERY`, `RELATIONSHIP` is required", e.getMessage)
      case _: Throwable => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}")
    }
  }

  @Test
  def testThrowsExceptionIfTwoValidReadOptionAreSet(): Unit = {
    try {
      ss.read.format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("labels", "Person")
        .option("relationship", "KNOWS")
        .load() // we need the action to be able to trigger the exception because of the changes in Spark 3
    } catch {
      case e: IllegalArgumentException =>
        assertEquals(
          "You need to specify just one of these options: 'gds', 'labels', 'query', 'relationship'",
          e.getMessage
        )
      case _: Throwable => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}")
    }
  }

  @Test
  def testThrowsExceptionIfThreeValidReadOptionAreSet(): Unit = {
    try {
      ss.read.format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("labels", "Person")
        .option("relationship", "KNOWS")
        .option("query", "MATCH (n) RETURN n")
        .load() // we need the action to be able to trigger the exception because of the changes in Spark 3
    } catch {
      case e: IllegalArgumentException =>
        assertEquals(
          "You need to specify just one of these options: 'gds', 'labels', 'query', 'relationship'",
          e.getMessage
        )
      case _: Throwable => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}")
    }
  }

  @Test
  def `should write nodes with string values into Neo4j`(): Unit = {
    val total = 10
    val ds = dfSingleFooString((1 to total).map(_.toString).toSeq)

    testType[String](ds, InternalTypeSystem.TYPE_SYSTEM.STRING())
  }

  @Test
  def `should write nodes with string array values into Neo4j`(): Unit = {
    val total = 10
    val ds = dfSingleFooStringArray((1 to total).map(i => Array(i.toString, i.toString)).toSeq)

    testArray[String](ds)
  }

  @Test
  def `should write nodes with int values into Neo4j`(): Unit = {
    val total = 10
    val ds = dfSingleFooInt((1 to total).toSeq)

    testType[Int](ds, InternalTypeSystem.TYPE_SYSTEM.INTEGER())
  }

  @Test
  def `should write nodes with date values into Neo4j`(): Unit = {
    val total = 5
    val ds = dfSingleFooDate((1 to total).map(i => java.sql.Date.valueOf("2020-01-0" + i)).toSeq)

    testType[java.sql.Date](ds, InternalTypeSystem.TYPE_SYSTEM.DATE())
  }

  @Test
  def `should write nodes with timestamp values into Neo4j`(): Unit = {
    val total = 5
    val ds = dfSingleFooTimestamp(
      (1 to total).map(i => java.sql.Timestamp.valueOf(s"2020-01-0$i 11:11:11.11")).toSeq
    )

    testType[java.sql.Timestamp](ds, InternalTypeSystem.TYPE_SYSTEM.LOCAL_DATE_TIME())
  }

  @Test
  def `should write nodes with int array values into Neo4j`(): Unit = {
    val total = 10
    val ds = dfSingleFooLongArray((1 to total).map(i => Array(i.toLong, i.toLong)).toSeq)

    testArray[Long](ds)
  }

  @Test
  def `should write nodes with point-2d values into Neo4j`(): Unit = {
    val total = 10
    val df = dfEmptyRowPoint2d(
      (1 to total).map(i => EmptyRow(Point2d(srid = 4326, x = Random.nextDouble(), y = Random.nextDouble()))).toSeq
    )

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.data AS data
        |""".stripMargin
    ).list().asScala
      .filter(r => r.get("data").hasType(InternalTypeSystem.TYPE_SYSTEM.POINT()))
      .map(r => {
        val point = r.get("data").asPoint()
        (point.srid(), point.x(), point.y())
      })
      .toSet
    val expected = df.collect()
      .map(r => tuplePoint2d(r.getStruct(0)))
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with point-2d array values into Neo4j`(): Unit = {
    val total = 10
    val df = dfEmptyRowSeqPoint2d(
      (1 to total)
        .map(i =>
          EmptyRow(Seq(
            Point2d(srid = 4326, x = Random.nextDouble(), y = Random.nextDouble()),
            Point2d(srid = 4326, x = Random.nextDouble(), y = Random.nextDouble())
          ))
        )
        .toSeq
    )

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.data AS data
        |""".stripMargin
    ).list().asScala
      .filter(r => r.get("data").hasType(InternalTypeSystem.TYPE_SYSTEM.LIST()))
      .map(r =>
        r.get("data")
          .asList.asScala
          .map(_.asInstanceOf[InternalPoint2D])
          .map(point => (point.srid(), point.x(), point.y()))
      )
      .toSet
    val expected = df.collect()
      .map(r => seqRow(r.get(0)).map(tuplePoint2d).toSeq)
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with point-3d values into Neo4j`(): Unit = {
    val total = 10
    val df = dfEmptyRowPoint3d(
      (1 to total)
        .map(i =>
          EmptyRow(Point3d(srid = 4979, x = Random.nextDouble(), y = Random.nextDouble(), z = Random.nextDouble()))
        )
        .toSeq
    )

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.data AS data
        |""".stripMargin
    ).list().asScala
      .filter(r => r.get("data").hasType(InternalTypeSystem.TYPE_SYSTEM.POINT()))
      .map(r => {
        val point = r.get("data").asPoint()
        (point.srid(), point.x(), point.y())
      })
      .toSet
    val expected = df.collect()
      .map(r => tuplePoint3d3(r.getStruct(0)))
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with point-3d array values into Neo4j`(): Unit = {
    val total = 10
    val df = dfEmptyRowSeqPoint3d(
      (1 to total)
        .map(i =>
          EmptyRow(Seq(
            Point3d(srid = 4979, x = Random.nextDouble(), y = Random.nextDouble(), z = Random.nextDouble()),
            Point3d(srid = 4979, x = Random.nextDouble(), y = Random.nextDouble(), z = Random.nextDouble())
          ))
        )
        .toSeq
    )

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.data AS data
        |""".stripMargin
    ).list().asScala
      .filter(r => r.get("data").hasType(InternalTypeSystem.TYPE_SYSTEM.LIST()))
      .map(r =>
        r.get("data")
          .asList.asScala
          .map(_.asInstanceOf[InternalPoint3D])
          .map(point => (point.srid(), point.x(), point.y(), point.z()))
      )
      .toSet
    val expected = df.collect()
      .map(r => seqRow(r.get(0)).map(tuplePoint3d4).toSeq)
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with map values into Neo4j`(): Unit = {
    val total = 10
    val ds = dfSingleFooMapStringInt((1 to total).map(i => Map("field" + i -> i)).toSeq)

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p
        |""".stripMargin
    ).list().asScala
      .filter(r => r.get("p").hasType(InternalTypeSystem.TYPE_SYSTEM.MAP()))
      .map(r => r.get("p").asMap().asScala)
      .toSet
    val expected = ds.collect().map(row => row.getMap[String, AnyRef](0))
      .map(map => map.map(t => (s"foo.${t._1}", t._2)).toMap)
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with duration values into Neo4j`(): Unit = {
    val total = 10
    val df = dfEmptyRowDuration(
      (1 to total).map(i => i.toLong).map(i =>
        EmptyRow(Duration(months = i, days = i, seconds = i, nanoseconds = i))
      ).toSeq
    )

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "BeanWithDuration")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:BeanWithDuration)
        |RETURN p.data AS data
        |""".stripMargin
    ).list().asScala
      .map(r => r.get("data").asIsoDuration())
      .map(data => (data.months, data.days, data.seconds, data.nanoseconds))
      .toSet

    val expected = df.collect()
      .map(r => tupleDuration(r.getStruct(0)))
      .toSet

    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with duration array values into Neo4j`(): Unit = {
    val total = 10
    val df = dfEmptyRowSeqDuration(
      (1 to total)
        .map(i => i.toLong)
        .map(i =>
          EmptyRow(Seq(
            Duration(months = i, days = i, seconds = i, nanoseconds = i),
            Duration(months = i, days = i, seconds = i, nanoseconds = i)
          ))
        )
        .toSeq
    )

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "BeanWithDuration")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:BeanWithDuration)
        |RETURN p.data AS data
        |""".stripMargin
    ).list().asScala
      .map(r =>
        r.get("data")
          .asList.asScala
          .map(_.asInstanceOf[IsoDuration])
          .map(data => (data.months, data.days, data.seconds, data.nanoseconds))
      )
      .toSet

    val expected = df.collect()
      .map(r => seqRow(r.get(0)).map(tupleDuration).toSeq)
      .toSet

    assertEquals(expected, records)
  }

  @Test
  def `should write nodes into Neo4j with points`(): Unit = {
    val total = 10
    val rand = Random
    val df = dfPerson(
      (1 to total)
        .map(i =>
          Person(
            name = "Andrea " + i,
            "Santurbano " + i,
            rand.nextInt(100),
            Some(Point3d(srid = 4979, x = 12.5811776, y = 41.9579492, z = 1.3))
          )
        )
        .toSeq
    )

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":Person: Customer")
      .save()

    val count = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:Person:Customer)
        |WHERE p.name STARTS WITH 'Andrea'
        |AND p.surname STARTS WITH 'Santurbano'
        |RETURN count(p) AS count
        |""".stripMargin
    ).single().get("count").asInt()
    assertEquals(total, count)

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:Person:Customer)
        |WHERE p.name STARTS WITH 'Andrea'
        |AND p.surname STARTS WITH 'Santurbano'
        |RETURN p.name AS name, p.surname AS surname, p.age AS age,
        | p.bornIn AS bornIn, p.livesIn AS livesIn
        |""".stripMargin
    ).list().asScala
      .filter(r => {
        val map: java.util.Map[String, Object] = r.asMap()
        (map.get("name").isInstanceOf[String]
        && map.get("surname").isInstanceOf[String]
        && map.get("livesIn").isInstanceOf[InternalPoint3D]
        && map.get("age").isInstanceOf[Long])
      })
    assertEquals(total, records.size)
  }

  @Test
  def `should write nodes into Neo4j with Time and LocalTime Types`(): Unit = {
    val total = 1
    val rand = Random
    val df = dfPersonTimeLocal(
      (1 to total)
        .map(i =>
          Person_TimeAndLocalTime(
            name = "Andrea",
            time = Time(value = "12:50:35.556000000+01:00"),
            localTime = LocalTimeValue(value = "12:50:35.556000000")
          )
        )
        .toSeq
    )

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("node.keys", "name")
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":Person_TimeAndLocalTime")
      .save()

    val count = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:Person_TimeAndLocalTime)
        |WHERE p.name STARTS WITH 'Andrea'
        |RETURN count(p) AS count
        |""".stripMargin
    ).single().get("count").asInt()
    assertEquals(total, count)

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:Person_TimeAndLocalTime)
        |WHERE p.name STARTS WITH 'Andrea'
        |RETURN p.name AS name, p.time AS time, p.localTime AS localTime
        |""".stripMargin
    ).list().asScala
      .filter(r => {
        val map: java.util.Map[String, Object] = r.asMap()
        (map.get("name").isInstanceOf[String]
        && map.get("time").isInstanceOf[OffsetTime]
        && map.get("localTime").isInstanceOf[LocalTime])
      })
    assertEquals(total, records.size)
  }

  @Test
  def `should throw an error because the node already exists`(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .executeWrite(tx =>
        tx.run("CREATE CONSTRAINT person_surname FOR (p:Person) REQUIRE p.surname IS UNIQUE").consume()
      )
    SparkConnectorScalaSuiteIT.session()
      .executeWrite(tx => tx.run("CREATE (p:Person{name: 'Andrea', surname: 'Santurbano'})").consume())

    val df = dfSimplePerson(Seq(SimplePerson("Andrea", Some("Santurbano"))))

    try {
      val thrown = the[SparkException] thrownBy {
        df.write
          .format(classOf[DataSource].getName)
          .mode(SaveMode.Append)
          .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
          .option("labels", "Person")
          .save() // we need the action to be able to trigger the exception because of the changes in Spark 3
      }

      assert(thrown.getMessage contains "org.neo4j.driver.exceptions.ClientException")
      val rootCause = ExceptionUtils.getRootCause(thrown)
      // root cause is not always returned as a ClientException so we pass it through pattern matching to remove flakiness
      rootCause match {
        case c: ClientException =>
          c.code() should be("Neo.ClientError.Schema.ConstraintValidationFailed")
        case _ =>
      }
    } finally {
      SparkConnectorScalaSuiteIT.session()
        .executeWrite(tx => tx.run("DROP CONSTRAINT person_surname").consume())
    }
  }

  @Test
  def `should update the node that already exists`(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .executeWrite(tx =>
        tx.run("CREATE CONSTRAINT person_surname FOR (p:Person) REQUIRE p.surname IS UNIQUE").consume()
      )
    SparkConnectorScalaSuiteIT.session()
      .executeWrite(tx => tx.run("CREATE (p:Person{name: 'Federico', surname: 'Santurbano'})").consume())

    val df = dfSimplePerson(Seq(SimplePerson("Andrea", Some("Santurbano"))))

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Person")
      .option("node.keys", "surname")
      .save()

    val nodeList = SparkConnectorScalaSuiteIT.session()
      .run(
        """MATCH (n:Person{surname: 'Santurbano'})
          |RETURN n
          |""".stripMargin
      )
      .list()
      .asScala
    assertEquals(1, nodeList.size)
    assertEquals("Andrea", nodeList.head.get("n").asNode().get("name").asString())

    SparkConnectorScalaSuiteIT.session()
      .executeWrite(tx => tx.run("DROP CONSTRAINT person_surname").consume())
  }

  @Test
  def `should skip null properties`(): Unit = {
    val df = dfSimplePerson(Seq(SimplePerson("Andrea", None)))

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Person")
      .save()

    val nodeList = SparkConnectorScalaSuiteIT.session()
      .run(
        """MATCH (n:Person{name: 'Andrea'})
          |RETURN n
          |""".stripMargin
      )
      .list()
      .asScala
    assertEquals(1, nodeList.size)
    val node = nodeList.head.get("n").asNode()
    assertFalse("surname should not exist", node.asMap().containsKey("surname"))
  }

  @Test
  def `should throw an error because SaveMode.Overwrite need node.keys`(): Unit = {
    val df = dfSimplePerson(Seq(SimplePerson("Andrea", Some("Santurbano"))))
    try {
      df.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("labels", "Person")
        .save() // we need the action to be able to trigger the exception because of the changes in Spark 3
    } catch {
      case illegalArgumentException: IllegalArgumentException => {
        assertTrue(illegalArgumentException.getMessage.equals(
          s"${Neo4jOptions.NODE_KEYS} is required when Save Mode is Overwrite"
        ))
      }
      case e: Throwable =>
        fail(s"should be thrown a ${classOf[IllegalArgumentException].getName} but is ${e.getClass.getSimpleName}")
    }
  }

  @Test
  def `should write within partitions`(): Unit = {
    val df = dfPerson((1 to 100).map(i => Person("Andrea " + i, "Santurbano " + i, 36, None)).toSeq)
      .repartition(10)

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":Person:Customer")
      .option("batch.size", "11")
      .save()

    val count = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (p:Person:Customer)
        |WHERE p.name STARTS WITH 'Andrea'
        |AND p.surname STARTS WITH 'Santurbano'
        |RETURN count(p) AS count
        |""".stripMargin
    ).single().get("count").asInt()
    assertEquals(100, count)

    val keys = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (p:Person:Customer)
        |WHERE p.name STARTS WITH 'Andrea'
        |AND p.surname STARTS WITH 'Santurbano'
        |RETURN DISTINCT keys(p) AS keys
        |""".stripMargin
    ).single().get("keys").asList()
    assertEquals(Set("name", "surname", "age"), keys.asScala.toSet)
  }

  @Test
  @Ignore("This won't work right now because we can't know if we are in a Write or Read context")
  def `should throw an exception for a read only query`(): Unit = {
    val df = dfPerson((1 to 100).map(i => Person("Andrea " + i, "Santurbano " + i, 36, None)).toSeq)

    try {
      df.write
        .mode(SaveMode.Overwrite)
        .format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("query", "MATCH (r:Read) RETURN r")
        .option("batch.size", "11")
        .save() // we need the action to be able to trigger the exception because of the changes in Spark 3
    } catch {
      case illegalArgumentException: IllegalArgumentException =>
        assertTrue(illegalArgumentException.getMessage.equals("Please provide a valid WRITE query"))
      case t: Throwable => fail(
          s"should be thrown a ${classOf[IllegalArgumentException].getName}, but it's ${t.getClass.getSimpleName}: ${t.getMessage}"
        )
    }
  }

  @Test
  def `should insert data with a custom query`(): Unit = {
    val df = dfPerson((1 to 100).map(i => Person("Andrea " + i, "Santurbano " + i, 36, None)).toSeq)

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query", "CREATE (n:MyNode{fullName: event.name + event.surname, age: event.age - 10})")
      .option("batch.size", "11")
      .save()

    val count = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (p:MyNode)
        |WHERE p.fullName CONTAINS 'Andrea'
        |AND p.fullName CONTAINS 'Santurbano'
        |AND p.age = 26
        |RETURN count(p) AS count
        |""".stripMargin
    ).single().get("count").asLong()
    assertEquals(df.count(), count)
  }

  @Test
  def `should handle unusual column names`(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .executeWrite(tx =>
        tx.run("CREATE CONSTRAINT instrument_name FOR (i:Instrument) REQUIRE i.name IS UNIQUE").consume()
      )

    val musicDf = dfMusician4Unusual(
      (12, "John Bonham", "Drums", "f``````oo"),
      (19, "John Mayer", "Guitar", "bar"),
      (32, "John Scofield", "Guitar", "ba` z"),
      (15, "John Butler", "Guitar", "qu   ux")
    )

    musicDf.write
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("relationship", "PLAYS")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.source.node.keys", "name")
      .option("relationship.source.node.properties", "fi``(╯°□°)╯︵ ┻━┻eld:field")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .option("relationship.target.save.mode", "Overwrite")
      .save()

    SparkConnectorScalaSuiteIT.session()
      .executeWrite(tx => tx.run("DROP CONSTRAINT instrument_name").consume())

    val musicDfCheck = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.target.labels", ":Instrument")
      .load()

    val size = musicDfCheck.count
    assertEquals(4, size)

    val res = musicDfCheck.orderBy("`source.name`").collectAsList()

    assertEquals("John Bonham", res.get(0).getString(4))
    assertEquals("f``````oo", res.get(0).getString(5))
    assertEquals("Drums", res.get(0).getString(8))

    assertEquals("John Butler", res.get(1).getString(4))
    assertEquals("qu   ux", res.get(1).getString(5))
    assertEquals("Guitar", res.get(1).getString(8))

    assertEquals("John Mayer", res.get(2).getString(4))
    assertEquals("bar", res.get(2).getString(5))
    assertEquals("Guitar", res.get(2).getString(8))

    assertEquals("John Scofield", res.get(3).getString(4))
    assertEquals("ba` z", res.get(3).getString(5))
    assertEquals("Guitar", res.get(3).getString(8))
  }

  @Test(expected = classOf[SparkException])
  def `should give error if native mode doesn't find a valid schema`(): Unit = {
    val musicDf = dfMusician3(
      (12, "John Bonham", "Drums"),
      (19, "John Mayer", "Guitar"),
      (32, "John Scofield", "Guitar"),
      (15, "John Butler", "Guitar")
    )

    try {
      musicDf.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Append)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("relationship", "PLAYS")
        .option("relationship.save.strategy", "NATIVE")
        .option("relationship.source.labels", ":Person")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Instrument")
        .option("relationship.target.save.mode", "Overwrite")
        .save() // we need the action to be able to trigger the exception because of the changes in Spark 3
    } catch {
      case sparkException: SparkException => {
        val clientException = ExceptionUtils.getRootCause(sparkException)
        assertTrue(clientException.getMessage.equals(
          "NATIVE write strategy requires a schema like: rel.[props], source.[props], target.[props]. " +
            "All of these columns are empty in the current schema."
        ))
        throw sparkException
      }
      case _: Throwable => fail(s"should be thrown a ${classOf[SparkException].getName}")
    }
  }

  @Test
  def `should write relations with KEYS mode`(): Unit = {
    val musicDf = dfMusician3(
      (12, "John Bonham", "Drums"),
      (19, "John Mayer", "Guitar"),
      (32, "John Scofield", "Guitar"),
      (15, "John Butler", "Guitar")
    )

    musicDf.repartition(1).write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.target.save.mode", "Overwrite")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "name:name")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save()

    val df2 = ss.read.format(classOf[DataSource].getName)
      .option("batch.size", 100)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship.nodes.map", "false")
      .option("relationship", "PLAYS")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.target.labels", ":Instrument")
      .load()

    assertEquals(4, df2.count())

    val res = df2.orderBy("`source.name`").collectAsList()

    assertEquals("John Bonham", res.get(0).getString(4))
    assertEquals("Drums", res.get(0).getString(7))

    assertEquals("John Butler", res.get(1).getString(4))
    assertEquals("Guitar", res.get(1).getString(7))

    assertEquals("John Mayer", res.get(2).getString(4))
    assertEquals("Guitar", res.get(2).getString(7))

    assertEquals("John Scofield", res.get(3).getString(4))
    assertEquals("Guitar", res.get(3).getString(7))
  }

  @Test
  def `should fail validating options if ErrorIfExists is used`(): Unit = {
    val musicDf = dfMusician3(
      (12, "John Bonham", "Drums"),
      (19, "John Mayer", "Guitar"),
      (32, "John Scofield", "Guitar"),
      (15, "John Butler", "Guitar")
    )

    try {
      musicDf.repartition(1).write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("relationship", "PLAYS")
        .option("relationship.source.save.mode", "ErrorIfExists")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.labels", ":Musician")
        .option("relationship.source.node.keys", "name:name")
        .option("relationship.target.labels", ":Instrument")
        .option("relationship.target.node.keys", "instrument:name")
        .save()
    } catch {
      case e: IllegalArgumentException =>
        assertEquals("Save mode 'ErrorIfExists' is not supported on Spark 3.0, use 'Append' instead.", e.getMessage)
      case _: Throwable => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}")
    }
  }

  @Test
  @Ignore("trying to recreate the deadlock issue")
  def `should give better errors if transaction fails`(): Unit = {
    val df = dfNameInstrument(List.fill(200)(("John Bonham", "Drums")) *)

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.target.save.mode", "Overwrite")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "name:name")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save()

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("transaction.retries", 0)
      .option("partitions", "10")
      .option("relationship", "PLAYS")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.target.save.mode", "Overwrite")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "name:name")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save()
  }

  def writeKeyModeRelationshipWriteDataSet(
    optionModifier: Map[String, String] => Map[String, String] = { m => m }
  ): DataFrame = {
    val musicDf = dfMusician5(
      (12, "John Bonham", "Drums", 2, true),
      (19, "John Mayer", "Guitar", 1, false),
      (32, "John Scofield", "Guitar", 3, true),
      (15, "John Butler", "Guitar", 4, false)
    )

    val options = Map(
      "url" -> SparkConnectorScalaSuiteIT.server.getBoltUrl,
      "relationship" -> "PLAYS",
      "relationship.source.save.mode" -> "Overwrite",
      "relationship.target.save.mode" -> "Overwrite",
      "relationship.save.strategy" -> "keys",
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

    resultDf.show(false)
    assertEquals(4, resultDf.count())

    val res = resultDf.orderBy("`source.name`").collectAsList()

    assertEquals("John Bonham", getByName[String](res.get(0), "source.name"))
    assertEquals("Drums", getByName[String](res.get(0), "target.name"))
    assertEquals("Drums", getByName[String](res.get(0), "rel.instrument"))
    assertEquals(12, getByName[Long](res.get(0), "rel.experience"))
    assertEquals(2, getByName[Long](res.get(0), "rel.avgRating"))
    assertThrows[IllegalArgumentException](
      "relationship should not have hasDiploma field",
      res.get(0).fieldIndex("rel.hasDiploma")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have rating field",
      res.get(0).fieldIndex("rel.rating")
    )
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(0).fieldIndex("rel.name"))

    assertEquals("John Butler", getByName[String](res.get(1), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(1), "target.name"))
    assertEquals("Guitar", getByName[String](res.get(1), "rel.instrument"))
    assertEquals(15, getByName[Long](res.get(1), "rel.experience"))
    assertEquals(4, getByName[Long](res.get(1), "rel.avgRating"))
    assertThrows[IllegalArgumentException](
      "relationship should not have hasDiploma field",
      res.get(1).fieldIndex("rel.hasDiploma")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have rating field",
      res.get(1).fieldIndex("rel.rating")
    )
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(1).fieldIndex("rel.name"))

    assertEquals("John Mayer", getByName[String](res.get(2), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(2), "target.name"))
    assertEquals("Guitar", getByName[String](res.get(2), "rel.instrument"))
    assertEquals(19, getByName[Long](res.get(2), "rel.experience"))
    assertEquals(1, getByName[Long](res.get(2), "rel.avgRating"))
    assertThrows[IllegalArgumentException](
      "relationship should not have hasDiploma field",
      res.get(2).fieldIndex("rel.hasDiploma")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have rating field",
      res.get(2).fieldIndex("rel.rating")
    )
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(2).fieldIndex("rel.name"))

    assertEquals("John Scofield", getByName[String](res.get(3), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(3), "target.name"))
    assertEquals("Guitar", getByName[String](res.get(3), "rel.instrument"))
    assertEquals(32, getByName[Long](res.get(3), "rel.experience"))
    assertEquals(3, getByName[Long](res.get(3), "rel.avgRating"))
    assertThrows[IllegalArgumentException](
      "relationship should not have hasDiploma field",
      res.get(3).fieldIndex("rel.hasDiploma")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have rating field",
      res.get(3).fieldIndex("rel.rating")
    )
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(3).fieldIndex("rel.name"))
  }

  @Test
  def `should write relations with KEYS mode with explicitly listed empty properties`(): Unit = {
    val resultDf = writeKeyModeRelationshipWriteDataSet({ options =>
      options + ("relationship.properties" -> "")
    })

    resultDf.show(false)
    assertEquals(4, resultDf.count())

    val res = resultDf.orderBy("`source.name`").collectAsList()

    assertEquals("John Bonham", getByName[String](res.get(0), "source.name"))
    assertEquals("Drums", getByName[String](res.get(0), "target.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have experience field",
      res.get(0).fieldIndex("rel.experience")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have hasDiploma field",
      res.get(0).fieldIndex("rel.hasDiploma")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have rating field",
      res.get(0).fieldIndex("rel.rating")
    )
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(0).fieldIndex("rel.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have instrument field",
      res.get(0).fieldIndex("rel.instrument")
    )

    assertEquals("John Butler", getByName[String](res.get(1), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(1), "target.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have experience field",
      res.get(1).fieldIndex("rel.experience")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have hasDiploma field",
      res.get(1).fieldIndex("rel.hasDiploma")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have rating field",
      res.get(1).fieldIndex("rel.rating")
    )
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(1).fieldIndex("rel.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have instrument field",
      res.get(1).fieldIndex("rel.instrument")
    )

    assertEquals("John Mayer", getByName[String](res.get(2), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(2), "target.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have experience field",
      res.get(2).fieldIndex("rel.experience")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have hasDiploma field",
      res.get(2).fieldIndex("rel.hasDiploma")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have rating field",
      res.get(2).fieldIndex("rel.rating")
    )
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(2).fieldIndex("rel.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have instrument field",
      res.get(2).fieldIndex("rel.instrument")
    )

    assertEquals("John Scofield", getByName[String](res.get(3), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(3), "target.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have experience field",
      res.get(3).fieldIndex("rel.experience")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have hasDiploma field",
      res.get(3).fieldIndex("rel.hasDiploma")
    )
    assertThrows[IllegalArgumentException](
      "relationship should not have rating field",
      res.get(3).fieldIndex("rel.rating")
    )
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(3).fieldIndex("rel.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have instrument field",
      res.get(3).fieldIndex("rel.instrument")
    )
  }

  @Test
  def `should write relations with KEYS mode with default properties`(): Unit = {
    val resultDf = writeKeyModeRelationshipWriteDataSet()

    resultDf.show(false)
    assertEquals(4, resultDf.count())

    val res = resultDf.orderBy("`source.name`").collectAsList()

    assertEquals("John Bonham", getByName[String](res.get(0), "source.name"))
    assertEquals("Drums", getByName[String](res.get(0), "target.name"))
    assertEquals(12, getByName[Long](res.get(0), "rel.experience"))
    assertEquals(true, getByName[Boolean](res.get(0), "rel.hasDiploma"))
    assertEquals(2, getByName[Long](res.get(0), "rel.rating"))
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(0).fieldIndex("rel.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have instrument field",
      res.get(0).fieldIndex("rel.instrument")
    )

    assertEquals("John Butler", getByName[String](res.get(1), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(1), "target.name"))
    assertEquals(15, getByName[Long](res.get(1), "rel.experience"))
    assertEquals(false, getByName[Boolean](res.get(1), "rel.hasDiploma"))
    assertEquals(4, getByName[Long](res.get(1), "rel.rating"))
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(1).fieldIndex("rel.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have instrument field",
      res.get(1).fieldIndex("rel.instrument")
    )

    assertEquals("John Mayer", getByName[String](res.get(2), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(2), "target.name"))
    assertEquals(19, getByName[Long](res.get(2), "rel.experience"))
    assertEquals(false, getByName[Boolean](res.get(2), "rel.hasDiploma"))
    assertEquals(1, getByName[Long](res.get(2), "rel.rating"))
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(2).fieldIndex("rel.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have instrument field",
      res.get(2).fieldIndex("rel.instrument")
    )

    assertEquals("John Scofield", getByName[String](res.get(3), "source.name"))
    assertEquals("Guitar", getByName[String](res.get(3), "target.name"))
    assertEquals(32, getByName[Long](res.get(3), "rel.experience"))
    assertEquals(true, getByName[Boolean](res.get(3), "rel.hasDiploma"))
    assertEquals(3, getByName[Long](res.get(3), "rel.rating"))
    assertThrows[IllegalArgumentException]("relationship should not have name field", res.get(3).fieldIndex("rel.name"))
    assertThrows[IllegalArgumentException](
      "relationship should not have instrument field",
      res.get(3).fieldIndex("rel.instrument")
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

    val musicDf = dfMusician4WithId(
      (1, 12, "John Henry Bonham", "Drums"),
      (2, 19, "John Mayer", "Guitar"),
      (3, 32, "John Scofield", "Guitar"),
      (4, 15, "John Butler", "Guitar")
    )

    musicDf.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship.nodes.map", "false")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.target.save.mode", "Overwrite")
      .option("relationship", "PLAYS")
      .option("relationship.properties", "experience")
      .option("relationship.save.strategy", "keys")
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
  def `should insert index while insert nodes`(): Unit = {
    val total = 10
    val ds = dfSurnameStrings((1 to total).map(_.toString).toSeq)

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":Person:Customer")
      .option("node.keys", "surname")
      .option("schema.optimization.type", "INDEX")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:Person:Customer)
        |RETURN p.surname AS surname
        |""".stripMargin
    ).list().asScala
      .map(r => r.asMap().asScala)
      .toSet
    val expected = ds.collect().map(row => Map("surname" -> row.getAs[String]("surname")))
      .toSet
    assertEquals(expected, records)

    val indexCount = SparkConnectorScalaSuiteIT.session().run(
      getIndexQueryCount
    )
      .single()
      .get("count")
      .asLong()
    assertEquals(1, indexCount)

    SparkConnectorScalaSuiteIT.session().run("DROP INDEX spark_INDEX_Person_surname")
  }

  private def getIndexQueryCount: String = {
    val (uniqueKey, uniqueCondition) =
      if (TestUtil.neo4jVersion(SparkConnectorScalaSuiteIT.session()) >= Versions.NEO4J_5) {
        ("owningConstraint", "owningConstraint IS NULL")
      } else {
        ("uniqueness", "uniqueness = 'NONUNIQUE'")
      }

    s"""SHOW INDEXES YIELD labelsOrTypes, properties, $uniqueKey
       |WHERE labelsOrTypes = ['Person'] AND properties = ['surname'] AND $uniqueCondition
       |RETURN count(*) AS count
       |""".stripMargin
  }

  private def getConstraintQueryCount: String = {
    val (uniqueKey, uniqueCondition) =
      if (TestUtil.neo4jVersion(SparkConnectorScalaSuiteIT.session()) >= Versions.NEO4J_5) {
        ("owningConstraint", "owningConstraint IS NOT NULL")
      } else {
        ("uniqueness", "uniqueness = 'UNIQUE'")
      }
    s"""SHOW INDEXES YIELD labelsOrTypes, properties, $uniqueKey
       |WHERE labelsOrTypes = ['Person'] AND properties = ['surname'] AND $uniqueCondition
       |RETURN count(*) AS count
       |""".stripMargin
  }

  @Test
  def `should create constraint when insert nodes`(): Unit = {
    val total = 10
    val ds = dfSurnameStrings((1 to total).map(_.toString).toSeq)

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":Person:Customer")
      .option("node.keys", "surname")
      .option("schema.optimization.type", "NODE_CONSTRAINTS")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:Person:Customer)
        |RETURN p.surname AS surname
        |""".stripMargin
    ).list().asScala
      .map(r => r.asMap().asScala)
      .toSet
    val expected = ds.collect().map(row => Map("surname" -> row.getAs[String]("surname")))
      .toSet
    assertEquals(expected, records)

    val constraintCount = SparkConnectorScalaSuiteIT.session().run(
      getConstraintQueryCount
    )
      .single()
      .get("count")
      .asLong()
    assertEquals(1, constraintCount)
    SparkConnectorScalaSuiteIT.session().run("DROP CONSTRAINT spark_NODE_CONSTRAINTS_Person_surname")
  }

  @Test
  def `should not create constraint when insert nodes because they already exist`(): Unit = {
    SparkConnectorScalaSuiteIT.session().run(
      "CREATE CONSTRAINT person_surname FOR (p:Person) REQUIRE (p.surname) IS UNIQUE"
    )
    val total = 10
    val ds = dfSurnameStrings((1 to total).map(_.toString).toSeq)

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":Person:Customer")
      .option("node.keys", "surname")
      .option("schema.optimization.type", "NODE_CONSTRAINTS")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:Person:Customer)
        |RETURN p.surname AS surname
        |""".stripMargin
    ).list().asScala
      .map(r => r.asMap().asScala)
      .toSet
    val expected = ds.collect().map(row => Map("surname" -> row.getAs[String]("surname")))
      .toSet
    assertEquals(expected, records)

    val constraintCount = SparkConnectorScalaSuiteIT.session().run(
      getConstraintQueryCount
    )
      .single()
      .get("count")
      .asLong()
    assertEquals(1, constraintCount)
    SparkConnectorScalaSuiteIT.session().run("DROP CONSTRAINT person_surname")
  }

  @Test
  def `should insert indexes while insert with query`(): Unit = {
    val total = 10
    val ds = dfSurnameStrings((1 to total).map(_.toString).toSeq)

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":Person:Customer")
      .option("node.keys", "surname")
      .option("schema.optimization.type", "INDEX")
      .save()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query", "CREATE (n:MyNode{fullName: event.name + event.surname, age: event.age - 10})")
      .option("batch.size", "11")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:Person:Customer)
        |RETURN p.surname AS surname
        |""".stripMargin
    ).list().asScala
      .map(r => r.asMap().asScala)
      .toSet
    val expected = ds.collect().map(row => Map("surname" -> row.getAs[String]("surname")))
      .toSet
    assertEquals(expected, records)

    val indexCount = SparkConnectorScalaSuiteIT.session()
      .run(getIndexQueryCount)
      .single()
      .get("count")
      .asLong()
    assertEquals(1, indexCount)

    SparkConnectorScalaSuiteIT.session().run("DROP INDEX spark_INDEX_Person_surname")
  }

  @Test
  def `should manage script passing the data to the executors`(): Unit = {
    val df = dfSimplePerson(Seq(SimplePerson("Andrea", Some("Santurbano")), SimplePerson("Davide", Some("Fantuzzi"))))
      .repartition(2)

    df.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option(
        "query",
        "CREATE (n:Person{fullName: event.name + ' ' + event.surname, age: scriptResult[0].age[event.name]})"
      )
      .option(
        "script",
        """CREATE INDEX person_surname FOR (p:Person) ON (p.surname);
          |CREATE CONSTRAINT product_name_sku FOR (p:Product)
          | REQUIRE (p.name, p.sku)
          | IS NODE KEY;
          |RETURN {Andrea: 36, Davide: 32} AS age;
          |""".stripMargin
      )
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
    val expected = df.count
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
    val musicDf = dfMusician3(data *)

    musicDf.write
      .mode(SaveMode.Overwrite)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "name")
      .option("relationship.target.save.mode", "match")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save

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
    val musicDf = dfMusician3(data *)

    musicDf.repartition(1).write
      .mode(SaveMode.Overwrite)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "match")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "name")
      .option("relationship.target.save.mode", "overwrite")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save

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
    val musicDf = dfMusician3WhoName(data *)

    musicDf.repartition(1).write
      .mode(SaveMode.Overwrite)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "overwrite")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "`who:name`")
      .option("relationship.target.save.mode", "overwrite")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save

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
    val data = dfOrderComplex(
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
    )
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
    val data = dfNestedMapTable(
      ("Foo", 1, Map("inner" -> Map("key" -> "innerValue"))),
      ("Bar", 1, Map("inner" -> Map("key" -> "innerValue1")))
    )
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
    junit.Assert.assertEquals(2L, count)
  }

  @Test
  def shouldFix502WithCollisions(): Unit = {
    val data = dfListMapTable(
      ("Foo", 1, ListMap("key.inner" -> Map("key" -> "innerValue"), "key" -> Map("inner.key" -> "value"))),
      ("Bar", 1, ListMap("key.inner" -> Map("key" -> "innerValue1"), "key" -> Map("inner.key" -> "value1")))
    )
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
    junit.Assert.assertEquals(2L, count)
  }

  @Test
  def shouldFix502WithCollisionsAndAggregateValues(): Unit = {
    val data = dfListMapTable(
      ("Foo", 1, ListMap("key.inner" -> Map("key" -> "innerValue"), "key" -> Map("inner.key" -> "value"))),
      ("Bar", 1, ListMap("key.inner" -> Map("key" -> "innerValue1"), "key" -> Map("inner.key" -> "value1")))
    )
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
    junit.Assert.assertEquals(2L, count)
  }

  @Test
  def doesNotWriteNodePropertiesToRelationship(): Unit = {
    val data = dfWatched(
      ("john", "The Matrix", "today"),
      ("jane", "Oppenheimer", "yesterday"),
      ("şaban", "Hababam Sınıfı", "two days ago")
    )
    data.write
      .mode(SaveMode.Append)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "WATCHED")
      .option("relationship.save.strategy", "keys")
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
    junit.Assert.assertEquals(
      List(
        Map("watch_time" -> "today"),
        Map("watch_time" -> "two days ago"),
        Map("watch_time" -> "yesterday")
      ),
      rows
    )
  }
}
