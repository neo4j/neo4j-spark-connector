package org.neo4j.spark

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime, ZoneOffset}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.junit.Assert._
import org.junit.Test
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}

import java.util.TimeZone
import scala.collection.JavaConverters._
import scala.collection.mutable.{Seq, ArraySeq}

class DataSourceReaderWithApocTSE extends SparkConnectorScalaBaseWithApocTSE {

  @Test
  def testReadNodeHasIdField(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {name: 'John'})")

    /**
     * utnaf: Since we can't be sure we are in total isolation, and the id is generated
     * internally by org.neo4j.neo4j, we just check that the <id> field is an integer and is greater
     * than -1
     */
    assertTrue(df.select("<id>").collectAsList().get(0).getLong(0) > -1)
  }

  @Test
  def testReadNodeHasLabelsField(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person:Customer {name: 'John'})")

    val result = df.select("<labels>").collectAsList().get(0).getAs[Seq[String]](0)

    assertEquals("Person", result.head)
    assertEquals("Customer", result(1))
  }

  @Test
  def testReadNodeHasUnusualLabelsField(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:`Foo Bar`:Person {name: 'John'})")

    val result = df.select("<labels>").collectAsList().get(0).getAs[Seq[String]](0)

    assertEquals(Set("Person", "Foo Bar"), result.toSet[String])
  }

  @Test
  def testReadNodeWithFieldWithDifferentTypes(): Unit = {
    val df: DataFrame = initTest("CREATE (p1:Person {id: 1, field: [12,34]}), (p2:Person {id: 2, field: 123})")

    val res = df.orderBy("id").collectAsList()

    assertEquals("[12,34]", res.get(0).get(3))
    assertEquals("123", res.get(1).get(3))
  }

  @Test
  def testReadNodeWithString(): Unit = {
    val name: String = "John"
    val df: DataFrame = initTest(s"CREATE (p:Person {name: '$name'})")

    assertEquals(name, df.select("name").collectAsList().get(0).getString(0))
  }

  @Test
  def testReadNodeWithLong(): Unit = {
    val age: Long = 42
    val df: DataFrame = initTest(s"CREATE (p:Person {age: $age})")

    assertEquals(age, df.select("age").collectAsList().get(0).getLong(0))
  }

  @Test
  def testReadNodeWithDouble(): Unit = {
    val score: Double = 3.14
    val df: DataFrame = initTest(s"CREATE (p:Person {score: $score})")

    assertEquals(score, df.select("score").collectAsList().get(0).getDouble(0), 0)
  }

  @Test
  def testReadNodeWithLocalTime(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {aTime: localtime({hour:12, minute: 23, second: 0, millisecond: 294})})")

    val result = df.select("aTime").collectAsList().get(0).getAs[GenericRowWithSchema](0)

    assertEquals("local-time", result.get(0))
    assertEquals("12:23:00.294", result.get(1))
  }

  @Test
  def testReadNodeWithTime(): Unit = {
    val timezone = TimeZone.getDefault
    val df: DataFrame = initTest(s"CREATE (p:Person {aTime: time({hour:12, minute: 23, second: 0, millisecond: 294})})")

    val result = df.select("aTime").collectAsList().get(0).getAs[GenericRowWithSchema](0)

    val localTime = LocalTime.of(12, 23, 0, 294000000)
    val expectedTime = OffsetTime.of(localTime, timezone.toZoneId.getRules.getOffset(Instant.now()))

    assertEquals("offset-time", result.get(0))
    assertEquals(expectedTime.toString, result.get(1))
  }

  @Test
  def testReadNodeWithLocalDateTime(): Unit = {
    val localDateTime = "2007-12-03T10:15:30"
    val df: DataFrame = initTest(s"CREATE (p:Person {aTime: localdatetime('$localDateTime')})")

    val result = df.select("aTime").collectAsList().get(0).getTimestamp(0)


    assertEquals(Timestamp.from(LocalDateTime.parse(localDateTime).toInstant(ZoneOffset.UTC)), result)
  }

  @Test
  def testReadNodeWithZonedDateTime(): Unit = {
    val datetime = "2015-06-24T12:50:35.556+01:00"
    val df: DataFrame = initTest(s"CREATE (p:Person {aTime: datetime('$datetime')})")

    val result = df.select("aTime").collectAsList().get(0).getTimestamp(0)


    assertEquals(Timestamp.from(OffsetDateTime.parse(datetime).toInstant), result)
  }

  @Test
  def testReadNodeWithPoint(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {location: point({x: 12.12, y: 13.13})})")

    val res = df.select("location").collectAsList().get(0).getAs[GenericRowWithSchema](0);

    assertEquals("point-2d", res.get(0))
    assertEquals(7203, res.get(1))
    assertEquals(12.12, res.get(2))
    assertEquals(13.13, res.get(3))
  }

  @Test
  def testReadNodeWithGeoPoint(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {location: point({longitude: 12.12, latitude: 13.13})})")

    val res = df.select("location").collectAsList().get(0).getAs[GenericRowWithSchema](0);

    assertEquals("point-2d", res.get(0))
    assertEquals(4326, res.get(1))
    assertEquals(12.12, res.get(2))
    assertEquals(13.13, res.get(3))
  }

  @Test
  def testReadNodeWithPoint3D(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {location: point({x: 12.12, y: 13.13, z: 1})})")

    val res = df.select("location").collectAsList().get(0).getAs[GenericRowWithSchema](0)

    assertEquals("point-3d", res.get(0))
    assertEquals(9157, res.get(1))
    assertEquals(12.12, res.get(2))
    assertEquals(13.13, res.get(3))
    assertEquals(1.0, res.get(4))
  }

  @Test
  def testReadNodeWithDate(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {born: date('2009-10-10')})")

    val list = df.select("born").collectAsList()
    val res = list.get(0).getDate(0)

    assertEquals(java.sql.Date.valueOf("2009-10-10"), res)
  }

  @Test
  def testReadNodeWithDuration(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {range: duration({days: 14, hours:16, minutes: 12})})")

    val list = df.select("range").collectAsList()
    val res = list.get(0).getAs[GenericRowWithSchema](0)

    assertEquals("duration", res(0))
    assertEquals(0L, res(1))
    assertEquals(14L, res(2))
    assertEquals(58320L, res(3))
    assertEquals(0, res(4))
    assertEquals("P0M14DT58320S", res(5))
  }

  @Test
  def testReadNodeWithStringArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {names: ['John', 'Doe']})")

    val res = df.select("names").collectAsList().get(0).getAs[Seq[String]](0)

    assertEquals("John", res.head)
    assertEquals("Doe", res(1))
  }

  @Test
  def testReadNodeWithLongArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {ages: [22, 23]})")

    val res = df.select("ages").collectAsList().get(0).getAs[Seq[Long]](0)

    assertEquals(22, res.head)
    assertEquals(23, res(1))
  }

  @Test
  def testReadNodeWithDoubleArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {scores: [22.33, 44.55]})")

    val res = df.select("scores").collectAsList().get(0).getAs[Seq[Double]](0)

    assertEquals(22.33, res.head, 0)
    assertEquals(44.55, res(1), 0)
  }

  @Test
  def testReadNodeWithLocalTimeArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {someTimes: [localtime({hour:12}), localtime({hour:1, minute: 3})]})")

    val res = df.select("someTimes").collectAsList().get(0).getAs[Seq[GenericRowWithSchema]](0)

    assertEquals("local-time", res.head.get(0))
    assertEquals("12:00:00", res.head.get(1))
    assertEquals("local-time", res(1).get(0))
    assertEquals("01:03:00", res(1).get(1))
  }

  @Test
  def testReadNodeWithBooleanArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {bools: [true, false]})")

    val res = df.select("bools").collectAsList().get(0).getAs[Seq[Boolean]](0)

    assertEquals(true, res.head)
    assertEquals(false, res(1))
  }

  @Test
  def testReadNodeWithPointArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {locations: [point({x: 11, y: 33.111}), point({x: 22, y: 44.222})]})")

    val res = df.select("locations").collectAsList().get(0).getAs[Seq[GenericRowWithSchema]](0)

    assertEquals("point-2d", res.head.get(0))
    assertEquals(7203, res.head.get(1))
    assertEquals(11.0, res.head.get(2))
    assertEquals(33.111, res.head.get(3))

    assertEquals("point-2d", res(1).get(0))
    assertEquals(7203, res(1).get(1))
    assertEquals(22.0, res(1).get(2))
    assertEquals(44.222, res(1).get(3))
  }

  @Test
  def testReadNodeWithGeoPointArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {locations: [point({longitude: 11, latitude: 33.111}), point({longitude: 22, latitude: 44.222})]})")

    val res = df.select("locations").collectAsList().get(0).getAs[Seq[GenericRowWithSchema]](0)

    assertEquals("point-2d", res.head.get(0))
    assertEquals(4326, res.head.get(1))
    assertEquals(11.0, res.head.get(2))
    assertEquals(33.111, res.head.get(3))

    assertEquals("point-2d", res(1).get(0))
    assertEquals(4326, res(1).get(1))
    assertEquals(22.0, res(1).get(2))
    assertEquals(44.222, res(1).get(3))
  }

  @Test
  def testReadNodeWithPoint3DArray(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {locations: [point({x: 11, y: 33.111, z: 12}), point({x: 22, y: 44.222, z: 99.1})]})")

    val res = df.select("locations").collectAsList().get(0).getAs[Seq[GenericRowWithSchema]](0)

    assertEquals("point-3d", res.head.get(0))
    assertEquals(9157, res.head.get(1))
    assertEquals(11.0, res.head.get(2))
    assertEquals(33.111, res.head.get(3))
    assertEquals(12.0, res.head.get(4))

    assertEquals("point-3d", res(1).get(0))
    assertEquals(9157, res(1).get(1))
    assertEquals(22.0, res(1).get(2))
    assertEquals(44.222, res(1).get(3))
    assertEquals(99.1, res(1).get(4))
  }

  @Test
  def testReadNodeWithArrayDate(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {dates: [date('2009-10-10'), date('2009-10-11')]})")

    val res = df.select("dates").collectAsList().get(0).getAs[Seq[java.sql.Date]](0)

    assertEquals(java.sql.Date.valueOf("2009-10-10"), res.head)
    assertEquals(java.sql.Date.valueOf("2009-10-11"), res(1))
  }

  @Test
  def testReadNodeWithArrayZonedDateTime(): Unit = {
    val datetime1 = "2015-06-24T12:50:35.556+01:00"
    val datetime2 = "2015-06-23T12:50:35.556+01:00"
    val df: DataFrame = initTest(s"""
     CREATE (p:Person {aTime: [
      datetime('$datetime1'),
      datetime('$datetime2')
     ]})
     """)

    val result = df.select("aTime").collectAsList().get(0).getAs[Seq[Timestamp]](0)

    assertEquals(Timestamp.from(OffsetDateTime.parse(datetime1).toInstant), result.head)
    assertEquals(Timestamp.from(OffsetDateTime.parse(datetime2).toInstant), result(1))
  }

  @Test
  def testReadNodeWithArrayDurations(): Unit = {
    val df: DataFrame = initTest(s"CREATE (p:Person {durations: [duration({months: 0.75}), duration({weeks: 2.5})]})")

    val res = df.select("durations").collectAsList().get(0).getAs[Seq[GenericRowWithSchema]](0)

    assertEquals("duration", res.head.get(0))
    assertEquals(0L, res.head.get(1))
    assertEquals(22L, res.head.get(2))
    assertEquals(71509L, res.head.get(3))
    assertEquals(500000000, res.head.get(4))
    assertEquals("P0M22DT71509.500000000S", res.head.get(5))

    assertEquals("duration", res(1).get(0))
    assertEquals(0L, res(1).get(1))
    assertEquals(17L, res(1).get(2))
    assertEquals(43200L, res(1).get(3))
    assertEquals(0, res(1).get(4))
    assertEquals("P0M17DT43200S", res(1).get(5))
  }

  @Test
  def testReadNodeWithEqualToFilter(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {name: 'John Doe'}),
      (p2:Person {name: 'Jane Doe'})
     """)

    val result = df.select("name").where("name = 'John Doe'").collectAsList()

    assertEquals(1, result.size())
    assertEquals("John Doe", result.get(0).getString(0))
  }

  @Test
  def testReadNodeWithNotEqualToFilter(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {name: 'John Doe'}),
      (p2:Person {name: 'Jane Doe'})
     """)

    val result = df.select("name").where("NOT name = 'John Doe'").collectAsList()

    assertEquals(1, result.size())
    assertEquals("Jane Doe", result.get(0).getString(0))
  }

  @Test
  def testReadNodeWithDifferentOperatorFilter(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {name: 'John Doe'}),
      (p2:Person {name: 'Jane Doe'})
     """)

    val result = df.select("name").where("name != 'John Doe'").collectAsList()

    assertEquals(1, result.size())
    assertEquals("Jane Doe", result.get(0).getString(0))
  }

  @Test
  def testReadNodeWithGtFilter(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {age: 19}),
      (p2:Person {age: 20}),
      (p3:Person {age: 21})
     """)

    val result = df.select("age").where("age > 20").collectAsList()

    assertEquals(1, result.size())
    assertEquals(21, result.get(0).getLong(0))
  }

  @Test
  def testReadNodeWithGteFilter(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {age: 19}),
      (p2:Person {age: 20}),
      (p3:Person {age: 21})
     """)

    val result = df.select("age").orderBy("age").where("age >= 20").collectAsList()

    assertEquals(2, result.size())
    assertEquals(20, result.get(0).getLong(0))
    assertEquals(21, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithGteFilterWithProp(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {score: 19, limit: 20}),
      (p2:Person {score: 20,  limit: 18}),
      (p3:Person {score: 21,  limit: 12})
     """)

    val result = df.select("score").orderBy("score").where("score >= limit").collectAsList()

    assertEquals(2, result.size())
    assertEquals(20, result.get(0).getLong(0))
    assertEquals(21, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithLtFilter(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: 41}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age < 40").collectAsList()

    assertEquals(1, result.size())
    assertEquals(39, result.get(0).getLong(0))
  }

  @Test
  def testReadNodeWithLteFilter(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: 41}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age <= 41").collectAsList()

    assertEquals(2, result.size())
    assertEquals(39, result.get(0).getLong(0))
    assertEquals(41, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithInFilter(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: 41}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age IN(41,43)").collectAsList()

    assertEquals(2, result.size())
    assertEquals(41, result.get(0).getLong(0))
    assertEquals(43, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithIsNullFilter(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: null}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").where("age IS NULL").collectAsList()

    assertEquals(1, result.size())
    assertNull(result.get(0).get(0))
  }

  @Test
  def testReadNodeWithIsNotNullFilter(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: null}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age IS NOT NULL").collectAsList()

    assertEquals(2, result.size())
    assertEquals(39, result.get(0).getLong(0))
    assertEquals(43, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithOrCondition(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: null}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age = 43 OR age = 39 OR age = 32").collectAsList()

    assertEquals(2, result.size())
    assertEquals(39, result.get(0).getLong(0))
    assertEquals(43, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithAndCondition(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {age: 39}),
      (p2:Person {age: null}),
      (p3:Person {age: 43})
     """)

    val result = df.select("age").orderBy("age").where("age >= 39 AND age <= 43").collectAsList()

    assertEquals(2, result.size())
    assertEquals(39, result.get(0).getLong(0))
    assertEquals(43, result.get(1).getLong(0))
  }

  @Test
  def testReadNodeWithStartsWith(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {name: 'John Mayer'}),
      (p2:Person {name: 'John Scofield'}),
      (p3:Person {name: 'John Butler'})
     """)

    val result = df.select("name").orderBy("name").where("name LIKE 'John%'").collectAsList()

    assertEquals(3, result.size())
    assertEquals("John Butler", result.get(0).getString(0))
    assertEquals("John Mayer", result.get(1).getString(0))
    assertEquals("John Scofield", result.get(2).getString(0))
  }

  @Test
  def testReadNodeWithEndsWith(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {name: 'John Mayer'}),
      (p2:Person {name: 'John Scofield'}),
      (p3:Person {name: 'John Butler'})
     """)

    val result = df.select("name").where("name LIKE '%Scofield'").collectAsList()

    assertEquals(1, result.size())
    assertEquals("John Scofield", result.get(0).getString(0))
  }

  @Test
  def testReadNodeWithContains(): Unit = {
    val df: DataFrame = initTest(s"""
     CREATE (p1:Person {name: 'John Mayer'}),
      (p2:Person {name: 'John Scofield'}),
      (p3:Person {name: 'John Butler'})
     """)

    val result = df.select("name").where("name LIKE '%ay%'").collectAsList()

    assertEquals(1, result.size())
    assertEquals("John Mayer", result.get(0).getString(0))
  }

  @Test
  def testRelFiltersWithMap(): Unit = {
    val fixtureQuery: String =
      """UNWIND range(1,100) as id
        |CREATE (p:Person {id:id,ids:[id,id]}) WITH collect(p) as people
        |UNWIND people as p1
        |UNWIND range(1,10) as friend
        |WITH p1, people[(p1.id + friend) % size(people)] as p2
        |CREATE (p1)-[:KNOWS]->(p2)
        |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteWithApocIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("relationship.nodes.map", "true")
      .option("relationship", "KNOWS")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Person")
      .load()

    assertEquals(1, df.filter("`<source>`.`id` = '14' AND `<target>`.`id` = '16'").collectAsList().size())
  }

  @Test
  def testRelFiltersWithoutMap(): Unit = {
    val fixtureQuery: String =
      """UNWIND range(1,100) as id
        |CREATE (p:Person {id:id,ids:[id,id]}) WITH collect(p) as people
        |UNWIND people as p1
        |UNWIND range(1,10) as friend
        |WITH p1, people[(p1.id + friend) % size(people)] as p2
        |CREATE (p1)-[:KNOWS]->(p2)
        |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteWithApocIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("relationship", "KNOWS")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Person")
      .load()

    assertEquals(1, df.filter("`source.id` = 14 AND `target.id` = 16").collectAsList().size())
  }

  @Test
  def testReadNodeRepartition(): Unit = {
    val fixtureQuery: String =
      """UNWIND range(1,100) as id
        |CREATE (p:Person {id:id,ids:[id,id]}) WITH collect(p) as people
        |UNWIND people as p1
        |UNWIND range(1,10) as friend
        |WITH p1, people[(p1.id + friend) % size(people)] as p2
        |CREATE (p1)-[:KNOWS]->(p2)
        |RETURN *
    """.stripMargin

    val df: DataFrame = initTest(fixtureQuery)
    val repartitionedDf = df.repartition(10)

    assertEquals(10, repartitionedDf.rdd.getNumPartitions)
    val numNode = repartitionedDf.collect().length
    assertEquals(100, numNode)
  }

  @Test
  def testRelationshipsFlatten(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteWithApocIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("relationship", "BOUGHT")
      .option("relationship.nodes.map", "false")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()

    val count = df.collectAsList()
      .asScala
      .filter(row => row.getAs[Long]("<rel.id>") >= 0
        && row.getAs[String]("<rel.type>") != null
        && row.getAs[Double]("rel.when") >= 0
        && row.getAs[Double]("rel.quantity") >= 0
        && row.getAs[Long]("<source.id>") >= 0
        && row.getAs[Long]("source.id") >= 0
        && !row.getAs[Seq[String]]("<source.labels>").isEmpty
        && row.getAs[String]("source.fullName") != null
        && row.getAs[Long]("<target.id>") >= 0
        && row.getAs[Double]("target.id") >= 0
        && !row.getAs[Seq[String]]("<target.labels>").isEmpty
        && row.getAs[String]("target.name") != null)
      .size
    assertEquals(total, count)
  }

  @Test
  def testRelationshipsMap(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteWithApocIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("relationship", "BOUGHT")
      .option("relationship.nodes.map", "true")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()

    val rows = df.collectAsList().asScala
    val count = rows
      .filter(row => row.getAs[Long]("<rel.id>") >= 0
        && row.getAs[String]("<rel.type>") != null
        && row.getAs[Double]("rel.when") >= 0
        && row.getAs[Double]("rel.quantity") >= 0
        && row.getAs[Map[String, String]]("<source>") != null
        && row.getAs[Map[String, String]]("<target>") != null)
      .size
    assertEquals(total, count)

    val countSourceMap = rows.map(row => row.getAs[Map[String, String]]("<source>"))
      .filter(row => row.keys == Set("id", "fullName", "<id>", "<labels>"))
      .size
    assertEquals(total, countSourceMap)
    val countTargetMap = rows.map(row => row.getAs[Map[String, String]]("<target>"))
      .filter(row => row.keys == Set("id", "name", "<id>", "<labels>"))
      .size
    assertEquals(total, countTargetMap)
  }

  @Test
  def testRelationshipsDifferentFieldValues(): Unit = {
    val fixtureQuery: String =
      s"""CREATE (pr1:Product {id: '1'})
         |CREATE (pr2:Product {id: 2})
         |CREATE (pe1:Person {id: '3'})
         |CREATE (pe2:Person {id: 4})
         |CREATE (pe1)-[:BOUGHT]->(pr1)
         |CREATE (pe2)-[:BOUGHT]->(pr2)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteWithApocIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("relationship.nodes.map", "false")
      .option("relationship", "BOUGHT")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .load()

    val res = df.sort("`source.id`").collectAsList()

    assertEquals("3", res.get(0).get(4))
    assertEquals("1", res.get(0).get(7))
    assertEquals("4", res.get(1).get(4))
    assertEquals("2", res.get(1).get(7))
  }

  @Test
  def testShouldReturnSamePropertiesForNodesWithMultipleLabels(): Unit = {
    val fixtureQuery: String =
      s"""CREATE (actor:Person:Actor {name: 'Keanu Reeves', born: 1964, actor: true})
         |CREATE (soccerPlayer:Person:SoccerPlayer {name: 'Zlatan Ibrahimović', born: 1981, soccerPlayer: true})
         |CREATE (writer:Person:Writer {name: 'Philip K. Dick', born: 1928, writer: true})
    """.stripMargin

    SparkConnectorScalaSuiteWithApocIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("labels", "Person")
      .load()
      .sort("name")

    val cols = df.columns.toSeq.sorted
    val expectedCols = Seq("name", "born", "actor",
        "soccerPlayer", "writer", "<id>", "<labels>")
      .sorted
    assertEquals(expectedCols, cols)

    val data = df.collect().toSeq
      .map(row => expectedCols.filterNot(_ == "<id>").map(col => {
        row.getAs[Any](col) match {
          case array: Array[String] => array.toList
          case null => null
          case other: Any => other
        }
      }))
    val expectedData = Seq(
      Seq(ArraySeq("Person", "Actor"), true, 1964, "Keanu Reeves", null, null),
      Seq(ArraySeq("Person", "Writer"), null, 1928, "Philip K. Dick", null, true),
      Seq(ArraySeq("Person", "SoccerPlayer"), null, 1981, "Zlatan Ibrahimović", true, null)
    ).toBuffer
    assertEquals(expectedData, data)
  }

  @Test
  def testShouldReturnSamePropertiesForNodesWithMultipleLabelsAndDifferentValues(): Unit = {
    val fixtureQuery: String =
      s"""CREATE (:Person { prop: 25 }),
         |(:Person:Player { prop: "hello" }),
         |(:Person:Player:Weirdo { prop: true })
    """.stripMargin

    SparkConnectorScalaSuiteWithApocIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("labels", "Person")
      .load()
      .sort("prop")

    val cols = df.columns.toSeq.sorted
    val expectedCols = Seq("prop", "<id>", "<labels>")
      .sorted
    assertEquals(expectedCols, cols)

    val data = df.collect().toSeq
      .map(row => expectedCols.filterNot(_ == "<id>").map(col => {
        row.getAs[Any](col) match {
          case array: Array[String] => array.toList
          case null => null
          case other: Any => other
        }
      }))
    val expectedData = Seq(
      Seq(ArraySeq("Person"), "25"),
      Seq(ArraySeq("Person", "Player"), "hello"),
      Seq(ArraySeq("Person", "Player", "Weirdo"), "true")
    )
    assertEquals(expectedData, data)
  }

  @Test
  def testReadNodesCustomPartitions(): Unit = {
    val fixtureQuery: String =
      """UNWIND range(1,100) as id
        |CREATE (p:Person:Customer {id: id, name: 'Person ' + id})
        |RETURN *
    """.stripMargin
    val fixture2Query: String =
      """UNWIND range(1,100) as id
        |CREATE (p:Employee:Customer {id: id, name: 'Person ' + id})
        |RETURN *
    """.stripMargin
    SparkConnectorScalaSuiteWithApocIT.driver.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })
    SparkConnectorScalaSuiteWithApocIT.driver.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixture2Query).consume()
        })

    val partitionedDf = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("labels", ":Person:Customer")
      .option("partitions", "5")
      .load()

    assertEquals(5, partitionedDf.rdd.getNumPartitions)
    assertEquals(100, partitionedDf.collect().map(_.getAs[Long]("id")).toSet.size)
  }

  @Test
  def testReadRelsCustomPartitions(): Unit = {
    val fixtureQuery: String =
      """UNWIND range(1,100) as id
        |CREATE (p:Person {id: id, name: 'Person ' + id})-[:BOUGHT{quantity: ceil(rand() * 100)}]->(:Product{id: id, name: 'Product ' + id})
        |RETURN *
    """.stripMargin
    SparkConnectorScalaSuiteWithApocIT.driver.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val partitionedDf = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("relationship.nodes.map", "true")
      .option("relationship", "BOUGHT")
      .option("relationship.source.labels", ":Person")
      .option("relationship.target.labels", ":Product")
      .option("partitions", "5")
      .load()

    assertEquals(5, partitionedDf.rdd.getNumPartitions)
    assertEquals(100, partitionedDf.collect().map(_.getAs[Long]("<rel.id>")).toSet.size)
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

    assertEquals(Set("foo", "bar"), df.columns.toSet)
    assertEquals(1, df.count())
  }

  private def initTest(query: String): DataFrame = {
    SparkConnectorScalaSuiteWithApocIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(query).consume()
        })

    ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
      .option("labels", "Person")
      .load()
  }
}
