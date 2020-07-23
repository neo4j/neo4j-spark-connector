package org.neo4j.spark

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.Assert._
import org.junit.Test
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.internal.{InternalPoint2D, InternalPoint3D}
import org.neo4j.driver.types.{IsoDuration, Type}
import org.neo4j.driver.{Result, Transaction, TransactionWork}

import scala.collection.JavaConverters._
import scala.util.Random

abstract class Neo4jType(`type`: String)
case class Duration(`type`: String = "duration",
                    months: Long,
                    days: Long,
                    seconds: Long,
                    nanoseconds: Long) extends Neo4jType(`type`)
case class Point2d(`type`: String = "point-2d",
                   srid: Int,
                   x: Double,
                   y: Double) extends Neo4jType(`type`)
case class Point3d(`type`: String = "point-3d",
                   srid: Int,
                   x: Double,
                   y: Double,
                   z: Double) extends Neo4jType(`type`)

case class Person(name: String, surname: String, age: Int, livesIn: Point3d)

case class SimplePerson(name: String, surname: String)

case class EmptyRow[T](data: T)

class DataSourceWriterTSE extends SparkConnectorScalaBaseTSE {
  val sparkSession = SparkSession.builder().getOrCreate()
  import sparkSession.implicits._

  private def testType[T](ds: DataFrame, neo4jType: Type): Unit = {
    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.foo AS foo
        |""".stripMargin).list().asScala
      .filter(r => r.get("foo").hasType(neo4jType))
      .map(r => r.asMap().asScala)
      .toSet
    val expected = ds.collect().map(row => Map("foo" -> row.getAs[T]("foo")))
      .toSet
    assertEquals(expected, records)
  }

  private def testArray[T](ds: DataFrame): Unit = {
    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.foo AS foo
        |""".stripMargin).list().asScala
      .filter(r => r.get("foo").hasType(InternalTypeSystem.TYPE_SYSTEM.LIST()))
      .map(r => r.get("foo").asList())
      .toSet
    val expected = ds.collect()
      .map(row => row.getList[T](0))
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with string values into Neo4j`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => i.toString)
      .toDF("foo")

    testType[String](ds, InternalTypeSystem.TYPE_SYSTEM.STRING())
  }

  @Test
  def `should write nodes with string array values into Neo4j`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => i.toString)
      .map(i => Array(i, i))
      .toDF("foo")

    testArray[String](ds)
  }

  @Test
  def `should write nodes with int values into Neo4j`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => i)
      .toDF("foo")

    testType[Int](ds, InternalTypeSystem.TYPE_SYSTEM.INTEGER())
  }

  @Test
  def `should write nodes with int array values into Neo4j`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => i.toLong)
      .map(i => Array(i, i))
      .toDF("foo")

    testArray[Long](ds)
  }

  @Test
  def `should write nodes with point-2d values into Neo4j`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => EmptyRow(Point2d(srid = 4326, x = Random.nextDouble(), y = Random.nextDouble())))
      .toDS()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.data AS data
        |""".stripMargin).list().asScala
      .filter(r => r.get("data").hasType(InternalTypeSystem.TYPE_SYSTEM.POINT()))
      .map(r => {
        val point = r.get("data").asPoint()
        (point.srid(), point.x(), point.y())
      })
      .toSet
    val expected = ds.collect()
      .map(point => (point.data.srid, point.data.x, point.data.y))
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with point-2d array values into Neo4j`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => EmptyRow(Seq(Point2d(srid = 4326, x = Random.nextDouble(), y = Random.nextDouble()),
        Point2d(srid = 4326, x = Random.nextDouble(), y = Random.nextDouble()))))
      .toDS()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.data AS data
        |""".stripMargin).list().asScala
      .filter(r => r.get("data").hasType(InternalTypeSystem.TYPE_SYSTEM.LIST()))
      .map(r => r.get("data")
        .asList.asScala
        .map(_.asInstanceOf[InternalPoint2D])
        .map(point => (point.srid(), point.x(), point.y())))
      .toSet
    val expected = ds.collect()
      .map(row => row.data.map(p => (p.srid, p.x, p.y)))
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with point-3d values into Neo4j`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => EmptyRow(Point3d(srid = 4979, x = Random.nextDouble(), y = Random.nextDouble(), z = Random.nextDouble())))
      .toDS()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.data AS data
        |""".stripMargin).list().asScala
      .filter(r => r.get("data").hasType(InternalTypeSystem.TYPE_SYSTEM.POINT()))
      .map(r => {
        val point = r.get("data").asPoint()
        (point.srid(), point.x(), point.y())
      })
      .toSet
    val expected = ds.collect()
      .map(point => (point.data.srid, point.data.x, point.data.y))
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with point-3d array values into Neo4j`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => EmptyRow(Seq(Point3d(srid = 4979, x = Random.nextDouble(), y = Random.nextDouble(), z = Random.nextDouble()),
        Point3d(srid = 4979, x = Random.nextDouble(), y = Random.nextDouble(), z = Random.nextDouble()))))
      .toDS()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p.data AS data
        |""".stripMargin).list().asScala
      .filter(r => r.get("data").hasType(InternalTypeSystem.TYPE_SYSTEM.LIST()))
      .map(r => r.get("data")
        .asList.asScala
        .map(_.asInstanceOf[InternalPoint3D])
        .map(point => (point.srid(), point.x(), point.y(), point.z())))
      .toSet
    val expected = ds.collect()
      .map(row => row.data.map(p => (p.srid, p.x, p.y, p.z)))
      .toSet
    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with map values into Neo4j`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => Map("field" + i -> i))
      .toDF("foo")

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":MyNode:MyLabel")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:MyNode:MyLabel)
        |RETURN p
        |""".stripMargin).list().asScala
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
    val ds = (1 to total)
      .map(i => i.toLong)
      .map(i => EmptyRow(Duration(months = i, days = i , seconds = i, nanoseconds = i)))
      .toDS()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "BeanWithDuration")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:BeanWithDuration)
        |RETURN p.data AS data
        |""".stripMargin).list().asScala
      .map(r => r.get("data").asIsoDuration())
      .map(data => (data.months, data.days, data.seconds, data.nanoseconds))
      .toSet

    val expected = ds.collect()
      .map(row => (row.data.months, row.data.days, row.data.seconds, row.data.nanoseconds))
      .toSet

    assertEquals(expected, records)
  }

  @Test
  def `should write nodes with duration array values into Neo4j`(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => i.toLong)
      .map(i => EmptyRow(Seq(Duration(months = i, days = i , seconds = i, nanoseconds = i),
        Duration(months = i, days = i , seconds = i, nanoseconds = i))))
      .toDS()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "BeanWithDuration")
      .save()

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:BeanWithDuration)
        |RETURN p.data AS data
        |""".stripMargin).list().asScala
      .map(r => r.get("data")
        .asList.asScala
        .map(_.asInstanceOf[IsoDuration])
        .map(data => (data.months, data.days, data.seconds, data.nanoseconds)))
      .toSet

    val expected = ds.collect()
      .map(row => row.data.map(data => (data.months, data.days, data.seconds, data.nanoseconds)))
      .toSet

    assertEquals(expected, records)
  }

  @Test
  def `should write nodes into Neo4j with points`(): Unit = {
    val total = 10
    val rand = Random
    val ds = (1 to total)
      .map(i => Person(name = "Andrea " + i, "Santurbano " + i, rand.nextInt(100),
        Point3d(srid = 4979, x = 12.5811776, y = 41.9579492, z = 1.3))).toDS()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":Person: Customer")
      .save()

    val count = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:Person:Customer)
        |WHERE p.name STARTS WITH 'Andrea'
        |AND p.surname STARTS WITH 'Santurbano'
        |RETURN count(p) AS count
        |""".stripMargin).single().get("count").asInt()
    assertEquals(total, count)

    val records = SparkConnectorScalaSuiteIT.session().run(
      """MATCH (p:Person:Customer)
        |WHERE p.name STARTS WITH 'Andrea'
        |AND p.surname STARTS WITH 'Santurbano'
        |RETURN p.name AS name, p.surname AS surname, p.age AS age,
        | p.bornIn AS bornIn, p.livesIn AS livesIn
        |""".stripMargin).list().asScala
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
  def `should throw an error because the node already exists`(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(new TransactionWork[Result] {
        override def execute(transaction: Transaction): Result = transaction.run("CREATE CONSTRAINT ON (p:Person) ASSERT p.surname IS UNIQUE")
      })
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(new TransactionWork[Result] {
        override def execute(transaction: Transaction): Result = transaction.run("CREATE (p:Person{name: 'Andrea', surname: 'Santurbano'})")
      })

    val ds = Seq(SimplePerson("Andrea", "Santurbano")).toDS()

    try {
      ds.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.ErrorIfExists)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("labels", "Person")
        .save()
    } catch {
      case sparkException: SparkException => {
        val clientException = ExceptionUtils.getRootCause(sparkException)
        assertTrue(clientException.getMessage.endsWith("already exists with label `Person` and property `surname` = 'Santurbano'"))
      }
      case _ => fail(s"should be thrown a ${classOf[SparkException].getName}")
    } finally {
      SparkConnectorScalaSuiteIT.session()
        .writeTransaction(new TransactionWork[Result] {
          override def execute(transaction: Transaction): Result = transaction.run("DROP CONSTRAINT ON (p:Person) ASSERT p.surname IS UNIQUE")
        })
    }
  }

  @Test
  def `should update the node that already exists`(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(new TransactionWork[Result] {
        override def execute(transaction: Transaction): Result = transaction.run("CREATE CONSTRAINT ON (p:Person) ASSERT p.surname IS UNIQUE")
      })
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(new TransactionWork[Result] {
        override def execute(transaction: Transaction): Result = transaction.run("CREATE (p:Person{name: 'Federico', surname: 'Santurbano'})")
      })

    val ds = Seq(SimplePerson("Andrea", "Santurbano")).toDS()

    ds.write
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
          |""".stripMargin)
      .list()
      .asScala
    assertEquals(1, nodeList.size)
    assertEquals("Andrea", nodeList.head.get("n").asNode().get("name").asString())


    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(new TransactionWork[Result] {
        override def execute(transaction: Transaction): Result = transaction.run("DROP CONSTRAINT ON (p:Person) ASSERT p.surname IS UNIQUE")
      })
  }

  @Test
  def `should skip null properties`(): Unit = {
    val ds = Seq(SimplePerson("Andrea", null)).toDS()

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Person")
      .save()

    val nodeList = SparkConnectorScalaSuiteIT.session()
      .run(
        """MATCH (n:Person{name: 'Andrea'})
          |RETURN n
          |""".stripMargin)
      .list()
      .asScala
    assertEquals(1, nodeList.size)
    val node = nodeList.head.get("n").asNode()
    assertFalse("surname should not exist", node.asMap().containsKey("surname"))
  }

  @Test
  def `should throw an error because SaveMode.Overwrite need node.keys`(): Unit = {
    val ds = Seq(SimplePerson("Andrea", "Santurbano")).toDS()
    try {
      ds.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("labels", "Person")
        .save()
    } catch {
      case illegalArgumentException: IllegalArgumentException => {
        assertTrue(illegalArgumentException.getMessage.equals(s"${Neo4jOptions.NODE_KEYS} is required when Save Mode is Overwrite"))
      }
      case _ => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}")
    }
  }

  @Test
  def `should write within partitions`(): Unit = {
    val ds = (1 to 100).map(i => Person("Andrea " + i, "Santurbano " + i, 36, null)).toDS()
      .repartition(10)

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.ErrorIfExists)
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
        |""".stripMargin).single().get("count").asInt()
    assertEquals(100, count)

    val keys = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (p:Person:Customer)
        |WHERE p.name STARTS WITH 'Andrea'
        |AND p.surname STARTS WITH 'Santurbano'
        |RETURN DISTINCT keys(p) AS keys
        |""".stripMargin).single().get("keys").asList()
    assertEquals(Set("name", "surname", "age"), keys.asScala.toSet)
  }

}