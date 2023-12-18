package org.neo4j.spark.service

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.junit.Assert._
import org.junit.runners.MethodSorters
import org.junit.{Before, FixMethodOrder, Test}
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.{Transaction, TransactionWork}
import org.neo4j.spark.converter.CypherToSparkTypeConverter
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Neo4jUtil, QueryType}
import org.neo4j.spark.{SparkConnectorScalaBaseWithApocTSE, SparkConnectorScalaSuiteWithApocIT}

import java.util
import java.util.UUID

@FixMethodOrder(MethodSorters.JVM)
class SchemaServiceWithApocTSE extends SparkConnectorScalaBaseWithApocTSE {

  @Before
  def beforeEach(): Unit = {
    SparkConnectorScalaSuiteWithApocIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
        })
  }

  @Test
  def testGetSchemaFromNodeBoolean(): Unit = {
    initTest("CREATE (p:Person {is_hero: true})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("is_hero", DataTypes.BooleanType))), schema)
  }

  @Test
  def testGetSchemaFromNodeString(): Unit = {
    initTest("CREATE (p:Person {name: 'John'})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("name", DataTypes.StringType))), schema)
  }

  @Test
  def testGetSchemaFromNodeLong(): Unit = {
    initTest("CREATE (p:Person {age: 93})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("age", DataTypes.LongType))), schema)
  }

  @Test
  def testGetSchemaFromNodeDouble(): Unit = {
    initTest("CREATE (p:Person {ratio: 43.120})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("ratio", DataTypes.DoubleType))), schema)
  }

  @Test
  def testGetSchemaFromNodePoint2D(): Unit = {
    initTest("CREATE (p:Person {location: point({x: 12.32, y: 49.32})})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("location", CypherToSparkTypeConverter.pointType))), schema)
  }

  @Test
  def testGetSchemaFromDate(): Unit = {
    initTest("CREATE (p:Person {born_on: date('1998-01-05')})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("born_on", DataTypes.DateType))), schema)
  }

  @Test
  def testGetSchemaFromDateTime(): Unit = {
    initTest("CREATE (p:Person {arrived_at: datetime('1998-01-05')})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("arrived_at", DataTypes.TimestampType))), schema)
  }

  @Test
  def testGetSchemaFromTime(): Unit = {
    initTest("CREATE (p:Person {arrived_at: time('125035.556+0100')})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("arrived_at", CypherToSparkTypeConverter.timeType))), schema)
  }

  @Test
  def testGetSchemaFromStringArray(): Unit = {
    initTest("CREATE (p:Person {names: ['John', 'Doe']})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("names", DataTypes.createArrayType(DataTypes.StringType)))), schema)
  }

  @Test
  def testGetSchemaFromDateArray(): Unit = {
    initTest("CREATE (p:Person {names: [date('2019-11-19'), date('2019-11-20')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("names", DataTypes.createArrayType(DataTypes.DateType)))), schema)
  }

  @Test
  def testGetSchemaFromTimestampArray(): Unit = {
    initTest("CREATE (p:Person {dates: [datetime('2019-11-19'), datetime('2019-11-20')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("dates", DataTypes.createArrayType(DataTypes.TimestampType)))), schema)
  }

  @Test
  def testGetSchemaFromTimeArray(): Unit = {
    initTest("CREATE (p:Person {dates: [time('125035.556+0100'), time('125125.556+0100')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("dates", DataTypes.createArrayType(CypherToSparkTypeConverter.timeType)))), schema)
  }

  @Test
  def testGetSchemaFromIntegerArray(): Unit = {
    initTest("CREATE (p:Person {ages: [42, 101]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(StructField("ages", DataTypes.createArrayType(DataTypes.LongType)))), schema)
  }

  @Test
  def testGetSchemaFromMultipleNodes(): Unit = {
    initTest(
      """
      CREATE (p1:Person {age: 31, name: 'Jane Doe'}),
        (p2:Person {name: 'John Doe', age: 33, location: null}),
        (p3:Person {age: 25, location: point({latitude: 12.12, longitude: 31.13})})
    """)

    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(getExpectedStructType(Seq(
      StructField("age", DataTypes.LongType),
      StructField("location", CypherToSparkTypeConverter.pointType),
      StructField("name", DataTypes.StringType)
    )), schema)
  }

  private def getExpectedStructType(structFields: Seq[StructField]): StructType = {
    val additionalFields: Seq[StructField] = Seq(
      StructField(Neo4jUtil.INTERNAL_LABELS_FIELD, DataTypes.createArrayType(DataTypes.StringType), nullable = true),
      StructField(Neo4jUtil.INTERNAL_ID_FIELD, DataTypes.LongType, nullable = false)
    )
    StructType(structFields.union(additionalFields).reverse)
  }

  private def initTest(query: String): Unit = {
    SparkConnectorScalaSuiteWithApocIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(query).consume()
        })
  }

  private def getSchema(options: java.util.Map[String, String]): StructType = {
    options.put(Neo4jOptions.URL, SparkConnectorScalaSuiteWithApocIT.server.getBoltUrl)
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)
    val uuid: String = UUID.randomUUID().toString

    val driverCache = new DriverCache(neo4jOptions.connection, uuid)
    val schemaService: SchemaService = new SchemaService(neo4jOptions, driverCache)

    val schema: StructType = schemaService.struct()
    schemaService.close()
    driverCache.close()


    schema
  }
}