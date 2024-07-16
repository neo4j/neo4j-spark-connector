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
package org.neo4j.spark.service

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.junit.Assert._
import org.junit.Before
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.runners.MethodSorters
import org.neo4j.driver.Transaction
import org.neo4j.driver.TransactionWork
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.spark.SparkConnectorScalaBaseTSE
import org.neo4j.spark.SparkConnectorScalaSuiteIT
import org.neo4j.spark.converter.CypherToSparkTypeConverter
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.util.QueryType

import java.util
import java.util.UUID

@FixMethodOrder(MethodSorters.JVM)
class SchemaServiceTSE extends SparkConnectorScalaBaseTSE {

  @Before
  def beforeEach(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("MATCH (n) DETACH DELETE n").consume()
        }
      )
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

    assertEquals(
      getExpectedStructType(Seq(StructField("names", DataTypes.createArrayType(DataTypes.StringType)))),
      schema
    )
  }

  @Test
  def testGetSchemaFromDateArray(): Unit = {
    initTest("CREATE (p:Person {names: [date('2019-11-19'), date('2019-11-20')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(
      getExpectedStructType(Seq(StructField("names", DataTypes.createArrayType(DataTypes.DateType)))),
      schema
    )
  }

  @Test
  def testGetSchemaFromTimestampArray(): Unit = {
    initTest("CREATE (p:Person {dates: [datetime('2019-11-19'), datetime('2019-11-20')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(
      getExpectedStructType(Seq(StructField("dates", DataTypes.createArrayType(DataTypes.TimestampType)))),
      schema
    )
  }

  @Test
  def testGetSchemaFromTimeArray(): Unit = {
    initTest("CREATE (p:Person {dates: [time('125035.556+0100'), time('125125.556+0100')]})")
    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(
      getExpectedStructType(Seq(StructField("dates", DataTypes.createArrayType(CypherToSparkTypeConverter.timeType)))),
      schema
    )
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
    """
    )

    val options: java.util.Map[String, String] = new util.HashMap[String, String]()
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")

    val schema = getSchema(options)

    assertEquals(
      getExpectedStructType(Seq(
        StructField("age", DataTypes.LongType),
        StructField("location", CypherToSparkTypeConverter.pointType),
        StructField("name", DataTypes.StringType)
      )),
      schema
    )
  }

  private def getExpectedStructType(structFields: Seq[StructField]): StructType = {
    val additionalFields: Seq[StructField] = Seq(
      StructField(Neo4jUtil.INTERNAL_LABELS_FIELD, DataTypes.createArrayType(DataTypes.StringType), nullable = true),
      StructField(Neo4jUtil.INTERNAL_ID_FIELD, DataTypes.LongType, nullable = false)
    )
    StructType(structFields.union(additionalFields).reverse)
  }

  private def initTest(query: String): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(query).consume()
        }
      )
  }

  private def getSchema(options: java.util.Map[String, String]): StructType = {
    options.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
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
