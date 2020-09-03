package org.neo4j.spark

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.Assert._
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.internal.{InternalPoint2D, InternalPoint3D}
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.types.{IsoDuration, Type}
import org.neo4j.driver.{Result, SessionConfig, Transaction, TransactionWork}

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

  val _expectedException: ExpectedException = ExpectedException.none

  @Rule
  def exceptionRule: ExpectedException = _expectedException

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
      .map(i => EmptyRow(Duration(months = i, days = i, seconds = i, nanoseconds = i)))
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
      .map(i => EmptyRow(Seq(Duration(months = i, days = i, seconds = i, nanoseconds = i),
        Duration(months = i, days = i, seconds = i, nanoseconds = i))))
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

  @Test(expected = classOf[SparkException])
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
        throw sparkException
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

  @Test
  def `should throw an exception for a read only query`(): Unit = {
    val ds = (1 to 100).map(i => Person("Andrea " + i, "Santurbano " + i, 36, null)).toDS()

    try {
      ds.write
        .format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("query", "MATCH (r:Read) RETURN r")
        .option("batch.size", "11")
        .save()
    } catch {
      case illegalArgumentException: IllegalArgumentException => assertTrue(illegalArgumentException.getMessage.equals("Please provide a valid WRITE query"))
      case t: Throwable => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}, but it's ${t.getClass.getSimpleName}")
    }
  }

  @Test
  def `should insert data with a custom query`(): Unit = {
    val ds = (1 to 100).map(i => Person("Andrea " + i, "Santurbano " + i, 36, null)).toDS()

    ds.write
      .format(classOf[DataSource].getName)
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
        |""".stripMargin).single().get("count").asLong()
    assertEquals(ds.count(), count)
  }

  @Test
  def `should handle unusual column names`(): Unit = {
    val musicDf = Seq(
      (12, "John Bonham", "Drums", "f``````oo"),
      (19, "John Mayer", "Guitar", "bar"),
      (32, "John Scofield", "Guitar", "ba` z"),
      (15, "John Butler", "Guitar", "qu   ux")
    ).toDF("experience", "name", "instrument", "fi``(╯°□°)╯︵ ┻━┻eld")

    musicDf.write
      .option("url", "bolt://localhost:7687")
      .format(classOf[DataSource].getName)
      .option("relationship", "PLAYS")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.properties", "field")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.source.node.keys", "name:name")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .option("relationship.target.save.mode", "Overwrite")
      .save()
  }

  @Test
  def `should give error if native mode doesn't find a valid schema`(): Unit = {
    val musicDf = Seq(
      (12, "John Bonham", "Drums", "f``````oo"),
      (19, "John Mayer", "Guitar", "bar"),
      (32, "John Scofield", "Guitar", "ba` z"),
      (15, "John Butler", "Guitar", "qu   ux")
    ).toDF("experience", "name", "instrument", "fi``(╯°□°)╯︵ ┻━┻eld")

    musicDf.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.save.strategy", "NATIVE")
      .option("relationship.source.labels", ":Person")
      .option("relationship.source.save.mode", "ErrorIfExists")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.save.mode", "ErrorIfExists")
      .save()

    ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Instrument")
      .load()
      .show()
  }

  @Test
  def `should read and write relations with append mode`(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
            tx.run(fixtureQuery)
            tx.commit()
          }
        })

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
        })

    val dfOriginal: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("relationship", "BOUGHT")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()

    dfOriginal.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.save.strategy", "NATIVE")
      .option("relationship.source.labels", ":Person")
      .option("relationship.source.save.mode", "ErrorIfExists")
      .option("relationship.target.labels", ":Product")
      .option("relationship.target.save.mode", "ErrorIfExists")
      .option("batch.size", "11")
      .save()

    // let's write again to prove that 2 relationship are being added
    dfOriginal.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.save.strategy", "NATIVE")
      .option("relationship.source.labels", ":Person")
      .option("relationship.source.save.mode", "ErrorIfExists")
      .option("relationship.target.labels", ":Product")
      .option("relationship.target.save.mode", "ErrorIfExists")
      .option("batch.size", "11")
      .save()

    val dfCopy = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()

    val dfOriginalCount = dfOriginal.count()
    assertEquals(dfOriginalCount * 2, dfCopy.count())

    val resSourceOrig = dfOriginal.select("`source.id`").orderBy("`source.id`").collectAsList()
    val resSourceCopy = dfCopy.select("`source.id`").orderBy("`source.id`").collectAsList()
    val resTargetOrig = dfOriginal.select("`target.id`").orderBy("`target.id`").collectAsList()
    val resTargetCopy = dfCopy.select("`target.id`").orderBy("`target.id`").collectAsList()

    for (i <- 0 until 1) {
      assertEquals(
        resSourceOrig.get(i).getLong(0),
        resSourceCopy.get(i).getLong(0)
      )
      assertEquals(
        resTargetOrig.get(i).getLong(0),
        resTargetCopy.get(i).getLong(0)
      )
    }

    assertEquals(
      2,
      dfCopy.where("`source.id` = 1").count()
    )
  }

  @Test
  def `should read and write relations with overwrite mode`(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
            tx.run(fixtureQuery)
            tx.commit()
          }
        })

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
        })

    val dfOriginal: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("relationship", "BOUGHT")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()

    dfOriginal.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.save.strategy", "NATIVE")
      .option("relationship.source.labels", ":Person")
      .option("relationship.source.save.mode", "ErrorIfExists")
      .option("relationship.target.labels", ":Product")
      .option("relationship.target.save.mode", "ErrorIfExists")
      .option("batch.size", "11")
      .save()

    // let's write the same thing again to prove there will be just one relation
    dfOriginal.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.save.strategy", "NATIVE")
      .option("relationship.source.labels", ":Person")
      .option("relationship.source.node.keys", "source.id:id")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.target.labels", ":Product")
      .option("relationship.target.node.keys", "target.id:id")
      .option("relationship.target.save.mode", "Overwrite")
      .option("batch.size", "11")
      .save()

    val dfCopy = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()

    val dfOriginalCount = dfOriginal.count()
    assertEquals(dfOriginalCount, dfCopy.count())

    for (i <- 0 until 1) {
      assertEquals(
        dfOriginal.select("`source.id`").collectAsList().get(i).getLong(0),
        dfCopy.select("`source.id`").collectAsList().get(i).getLong(0)
      )
      assertEquals(
        dfOriginal.select("`target.id`").collectAsList().get(i).getLong(0),
        dfCopy.select("`target.id`").collectAsList().get(i).getLong(0)
      )
    }

    assertEquals(
      1,
      dfCopy.where("`source.id` = 1").count()
    )
  }

  @Test
  def `should read and write relations with MATCH and node keys`(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
            tx.run(fixtureQuery)
            tx.commit()
          }
        })

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
            tx.run(fixtureQuery)
            tx.commit()
          }
        })

    val dfOriginal: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("relationship", "BOUGHT")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()

    dfOriginal.write
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.save.strategy", "NATIVE")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .option("relationship.source.node.keys", "source.id:id")
      .option("relationship.target.node.keys", "target.id:id")
      .option("relationship.source.save.mode", "Match")
      .option("relationship.target.save.mode", "Match")
      .option("batch.size", "11")
      .save()

    val dfCopy = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()

    for (i <- 0 until 1) {
      assertEquals(
        dfOriginal.select("`source.id`").collectAsList().get(i).getLong(0),
        dfCopy.select("`source.id`").collectAsList().get(i).getLong(0)
      )
      assertEquals(
        dfOriginal.select("`target.id`").collectAsList().get(i).getLong(0),
        dfCopy.select("`target.id`").collectAsList().get(i).getLong(0)
      )
    }
  }

  @Test
  def `should read and write relations with MERGE and node keys`(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * $total, name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[Unit] {
          override def execute(tx: Transaction): Unit = {
            tx.run("MATCH (n) DETACH DELETE n")
            tx.run(fixtureQuery)
            tx.commit()
          }
        })

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
        })

    val dfOriginal: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("relationship", "BOUGHT")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()

    dfOriginal.write
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.save.strategy", "NATIVE")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .option("relationship.source.node.keys", "source.id:id")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.target.node.keys", "target.id:id")
      .option("relationship.target.save.mode", "Overwrite")
      .option("batch.size", "11")
      .save()

    val dfCopy = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("relationship", "SOLD")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()

    for (i <- 0 until 1) {
      assertEquals(
        dfOriginal.select("`source.id`").collectAsList().get(i).getLong(0),
        dfCopy.select("`source.id`").collectAsList().get(i).getLong(0)
      )
      assertEquals(
        dfOriginal.select("`target.id`").collectAsList().get(i).getLong(0),
        dfCopy.select("`target.id`").collectAsList().get(i).getLong(0)
      )
    }
  }

  @Test
  def `should write relations with KEYS mode`(): Unit = {
    val musicDf = Seq(
      (12, "John Bonham", "Drums"),
      (19, "John Mayer", "Guitar"),
      (32, "John Scofield", "Guitar"),
      (15, "John Butler", "Guitar")
    ).toDF("experience", "name", "instrument")

    musicDf.write
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.source.save.mode", "ErrorIfExists")
      .option("relationship.target.save.mode", "ErrorIfExists")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "name:name")
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

    assertEquals(4, df2.count())

    val result = df2.select("`source.name`").orderBy("`source.name`").collectAsList()

    assertEquals("John Bonham", result.get(0).getString(0))
  }

  @Test
  def `should write relations with KEYS mode with props`(): Unit = {
    val musicDf = Seq(
      (12, "John Bonham", "Drums"),
      (19, "John Mayer", "Guitar"),
      (32, "John Scofield", "Guitar"),
      (15, "John Butler", "Guitar")
    ).toDF("experience", "name", "instrument")

    musicDf.write
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "PLAYS")
      .option("relationship.source.save.mode", "ErrorIfExists")
      .option("relationship.target.save.mode", "ErrorIfExists")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.properties", "name:name")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.properties", "instrument:name")
      .save()

    val df2 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship.nodes.map", "false")
      .option("relationship", "PLAYS")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.target.labels", ":Instrument")
      .load()

    assertEquals(4, df2.count())

    val result = df2.select("`source.name`").orderBy("`source.name`").collectAsList()

    assertEquals("John Bonham", result.get(0).getString(0))
  }

  @Test
  def `should read and write relations with node overwrite mode`(): Unit = {
    val fixtureQuery: String =
      s"""CREATE (m:Musician {id: 1, name: "John Bonham"})
         |CREATE (i:Instrument {name: "Drums"})
         |CREATE (m)-[:PLAYS {experience: 10}]->(i)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val musicDf = Seq(
      (1, 12, "John Henry Bonham", "Drums"),
      (2, 19, "John Mayer", "Guitar"),
      (3, 32, "John Scofield", "Guitar"),
      (4, 15, "John Butler", "Guitar")
    ).toDF("id", "experience", "name", "instrument")

    musicDf.write
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship.nodes.map", "false")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.target.save.mode", "Overwrite")
      .option("relationship", "PLAYS")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "id:id")
      .option("relationship.source.node.properties", "name:name")
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
  def `should read relations and write relation with match mode`(): Unit = {
    val fixtureQuery: String =
      s"""CREATE (m:Musician {name: "John Bonham", age: 32})
         |CREATE (i:Instrument {name: "Drums"})
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val musicDf = Seq(
      (12, 32, "John Bonham", "Drums")
    ).toDF("experience", "age", "name", "instrument")

    musicDf.write
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("relationship.nodes.map", "false")
      .option("relationship", "PLAYS")
      .option("relationship.source.save.mode", "Match")
      .option("relationship.target.save.mode", "Match")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.source.node.keys", "name:name,age:age")
      .option("relationship.target.labels", ":Instrument")
      .option("relationship.target.node.keys", "instrument:name")
      .save()

    val df2 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("relationship.nodes.map", "false")
      .option("relationship", "PLAYS")
      .option("relationship.source.labels", ":Musician")
      .option("relationship.target.labels", ":Instrument")
      .load()

    val experience = df2.select("`source.age`").where("`source.name` = 'John Bonham'")
      .collectAsList().get(0).getLong(0)

    assertEquals(32, experience)
  }

}