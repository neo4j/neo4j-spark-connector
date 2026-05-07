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
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DayTimeIntervalType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.YearMonthIntervalType
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assume
import org.junit.BeforeClass
import org.junit.Test
import org.neo4j.driver.types.IsoDuration
import org.neo4j.spark.util.ConstraintsOptimizationType
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.SchemaConstraintsOptimizationType

import java.sql.Date
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.TimeZone

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.math.Ordering.Implicits.infixOrderingOps

object DataSourceSchemaWriterTSE {

  @BeforeClass
  def checkNeo4jVersion() {
    Assume.assumeTrue(TestUtil.neo4jVersion(SparkConnectorScalaSuiteIT.session()) >= Versions.NEO4J_5_13)
  }
}

class DataSourceSchemaWriterTSE extends SparkConnectorScalaBaseTSE {
  val timeZoneLock = "UTC" // to make TIMESTAMP_NTZ tests deterministic

  final private val SHOW_CONSTRAINTS_QUERY =
    """|SHOW CONSTRAINTS
       |YIELD name, type, entityType, labelsOrTypes, properties, ownedIndex""".stripMargin

  final private val NODE_UNIQUENESS_SHOW_CONSTRAINTS_QUERY =
    """|SHOW CONSTRAINTS
       |YIELD name, type AS ptype, entityType, labelsOrTypes, properties, ownedIndex
       |RETURN name, entityType, labelsOrTypes, properties, ownedIndex,
       |CASE ptype
       |  WHEN "UNIQUENESS" THEN "NODE_PROPERTY_UNIQUENESS"
       |  ELSE ptype
       |END AS type""".stripMargin

  final private val RELATIONSHIP_UNIQUENESS_SHOW_CONSTRAINTS_QUERY =
    """|SHOW CONSTRAINTS
       |YIELD name, type AS ptype, entityType, labelsOrTypes, properties, ownedIndex
       |RETURN name, entityType, labelsOrTypes, properties, ownedIndex,
       |CASE ptype
       |  WHEN "RELATIONSHIP_UNIQUENESS" THEN "RELATIONSHIP_PROPERTY_UNIQUENESS"
       |  ELSE ptype
       |END AS type""".stripMargin

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("DataSourceWriterTSE")
    .config("spark.sql.session.timeZone", timeZoneLock) // to make TIMESTAMP_NTZ tests deterministic
    .getOrCreate()

  import sparkSession.implicits._

  private def mapData(data: Any): Any = data match {
    case null                 => null
    case a: Array[_]          => a.toSeq.map(mapData)
    case l: java.util.List[_] => l.asScala.toSeq.map(mapData)
    case d: LocalDate         => Date.valueOf(d)
    case zdt: ZonedDateTime   => Timestamp.from(zdt.toInstant)
    case any: Any             => any
  }

  private val schemaOptimization = SchemaConstraintsOptimizationType.values
    .filterNot(_ == SchemaConstraintsOptimizationType.NONE)
    .mkString(",")

  @Test
  def shouldApplySchemaForNodes(): Unit = {
    Assume.assumeTrue(
      "Skipping: requires Neo4j Enterprise Edition",
      TestUtil.isEnterpriseEdition(SparkConnectorScalaSuiteIT.session())
    )
    val (expectedNode: Map[_root_.java.lang.String, Any], df: DataFrame) = createNodesDataFrameWithNotNullColumns

    df
      .write
      .mode(SaveMode.Append)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":NodeWithSchema")
      .option(Neo4jOptions.SCHEMA_OPTIMIZATION, schemaOptimization)
      .save()
    val count: Long = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (n:NodeWithSchema)
        |RETURN count(n)
        |""".stripMargin
    )
      .single()
      .get(0)
      .asLong()

    assertEquals(1L, count)

    val expectedSchema = Seq(
      Map(
        "name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-boolean",
        "type" -> "NODE_PROPERTY_EXISTENCE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("boolean"),
        "propertyType" -> null
      ),
      Map(
        "name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-float",
        "type" -> "NODE_PROPERTY_EXISTENCE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("float"),
        "propertyType" -> null
      ),
      Map(
        "name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-int",
        "type" -> "NODE_PROPERTY_EXISTENCE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("int"),
        "propertyType" -> null
      ),
      Map(
        "name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-string",
        "type" -> "NODE_PROPERTY_EXISTENCE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("string"),
        "propertyType" -> null
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-boolean",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("boolean"),
        "propertyType" -> "BOOLEAN"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-booleanArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("booleanArray"),
        "propertyType" -> "LIST<BOOLEAN NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-date",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("date"),
        "propertyType" -> "DATE"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-dateArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("dateArray"),
        "propertyType" -> "LIST<DATE NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-float",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("float"),
        "propertyType" -> "FLOAT"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-floatArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("floatArray"),
        "propertyType" -> "LIST<FLOAT NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-int",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("int"),
        "propertyType" -> "INTEGER"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-intArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("intArray"),
        "propertyType" -> "LIST<INTEGER NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-localDateTime",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("localDateTime"),
        "propertyType" -> "LOCAL DATETIME"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-localDateTimeArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("localDateTimeArray"),
        "propertyType" -> "LIST<LOCAL DATETIME NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-string",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("string"),
        "propertyType" -> "STRING"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-stringArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("stringArray"),
        "propertyType" -> "LIST<STRING NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-zonedDateTime",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("zonedDateTime"),
        "propertyType" -> "ZONED DATETIME"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-zonedDateTimeArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("zonedDateTimeArray"),
        "propertyType" -> "LIST<ZONED DATETIME NOT NULL>"
      )
    )

    val keys = Seq("name", "type", "entityType", "labelsOrTypes", "properties", "propertyType")
    val actualSchema = SparkConnectorScalaSuiteIT.session()
      .run(s"SHOW CONSTRAINTS YIELD ${keys.mkString(", ")} RETURN * ORDER BY name")
      .list()
      .asScala
      .map(r => keys.map(key => (key, mapData(r.get(key).asObject()))).toMap)
      .toSeq
    assertEquals(expectedSchema, actualSchema)

    val actualNode: Map[String, Any] = SparkConnectorScalaSuiteIT.session()
      .readTransaction(tx =>
        tx.run("MATCH (n:NodeWithSchema) RETURN n")
          .list()
          .asScala
          .map(_.get("n").asNode())
          .map(_.asMap())
      )
      .head
      .asScala
      .mapValues(mapData)
      .toMap

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def shouldApplySchemaAndNodeKeysForNodes(): Unit = {
    Assume.assumeTrue(
      "Skipping: requires Neo4j Enterprise Edition",
      TestUtil.isEnterpriseEdition(SparkConnectorScalaSuiteIT.session())
    )
    val (expectedNode: Map[_root_.java.lang.String, Any], df: DataFrame) = createNodesDataFrameWithNotNullColumns
    df.write
      .mode(SaveMode.Overwrite)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":NodeWithSchema")
      .option(Neo4jOptions.SCHEMA_OPTIMIZATION, schemaOptimization)
      .option(Neo4jOptions.SCHEMA_OPTIMIZATION_NODE_KEY, ConstraintsOptimizationType.KEY.toString)
      .option("node.keys", "int,string")
      .save()
    val count: Long = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (n:NodeWithSchema)
        |RETURN count(n)
        |""".stripMargin
    )
      .single()
      .get(0)
      .asLong()
    assertEquals(1L, count)

    val expectedSchema = Seq(
      Map(
        "name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-boolean",
        "type" -> "NODE_PROPERTY_EXISTENCE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("boolean"),
        "propertyType" -> null
      ),
      Map(
        "name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-float",
        "type" -> "NODE_PROPERTY_EXISTENCE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("float"),
        "propertyType" -> null
      ),
      Map(
        "name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-int",
        "type" -> "NODE_PROPERTY_EXISTENCE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("int"),
        "propertyType" -> null
      ),
      Map(
        "name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-string",
        "type" -> "NODE_PROPERTY_EXISTENCE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("string"),
        "propertyType" -> null
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-boolean",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("boolean"),
        "propertyType" -> "BOOLEAN"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-booleanArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("booleanArray"),
        "propertyType" -> "LIST<BOOLEAN NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-date",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("date"),
        "propertyType" -> "DATE"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-dateArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("dateArray"),
        "propertyType" -> "LIST<DATE NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-float",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("float"),
        "propertyType" -> "FLOAT"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-floatArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("floatArray"),
        "propertyType" -> "LIST<FLOAT NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-int",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("int"),
        "propertyType" -> "INTEGER"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-intArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("intArray"),
        "propertyType" -> "LIST<INTEGER NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-localDateTime",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("localDateTime"),
        "propertyType" -> "LOCAL DATETIME"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-localDateTimeArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("localDateTimeArray"),
        "propertyType" -> "LIST<LOCAL DATETIME NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-string",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("string"),
        "propertyType" -> "STRING"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-stringArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("stringArray"),
        "propertyType" -> "LIST<STRING NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-zonedDateTime",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("zonedDateTime"),
        "propertyType" -> "ZONED DATETIME"
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-zonedDateTimeArray",
        "type" -> "NODE_PROPERTY_TYPE",
        "entityType" -> "NODE",
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "properties" -> Seq("zonedDateTimeArray"),
        "propertyType" -> "LIST<ZONED DATETIME NOT NULL>"
      ),
      Map(
        "name" -> "spark_NODE_KEY-CONSTRAINT_NodeWithSchema_int-string",
        "propertyType" -> null,
        "properties" -> Seq("int", "string"),
        "labelsOrTypes" -> Seq("NodeWithSchema"),
        "entityType" -> "NODE",
        "type" -> "NODE_KEY"
      )
    )

    val keys = Seq("name", "type", "entityType", "labelsOrTypes", "properties", "propertyType")
    val actualSchema = SparkConnectorScalaSuiteIT.session()
      .run(s"SHOW CONSTRAINTS YIELD ${keys.mkString(", ")} RETURN * ORDER BY name")
      .list()
      .asScala
      .map(r => keys.map(key => (key, mapData(r.get(key).asObject()))).toMap)
      .toSeq
    assertEquals(expectedSchema, actualSchema)

    val actualNode: Map[String, Any] = SparkConnectorScalaSuiteIT.session()
      .readTransaction(tx =>
        tx.run("MATCH (n:NodeWithSchema) RETURN n")
          .list()
          .asScala
          .map(_.get("n").asNode())
          .map(_.asMap())
      )
      .head
      .asScala
      .mapValues(mapData)
      .toMap

    assertEquals(expectedNode, actualNode)
  }

  private def createNodesDataFrameWithNotNullColumns: (Map[String, Any], DataFrame) = {
    TimeZone.setDefault(TimeZone.getTimeZone(timeZoneLock))

    val colNames = Array(
      "string",
      "int",
      "boolean",
      "float",
      "date",
      "localDateTime",
      "zonedDateTime",
      "stringArray",
      "intArray",
      "booleanArray",
      "floatArray",
      "dateArray",
      "localDateTimeArray",
      "zonedDateTimeArray"
    )
    val row = (
      "Foo",
      1,
      false,
      1.1,
      Date.valueOf("2023-11-22"),
      LocalDateTime.of(2023, 11, 22, 12, 12, 12),
      Timestamp.valueOf(s"2020-11-22 11:11:11.11"),
      Seq("Foo1", "Foo2"),
      Seq(1, 2),
      Seq(true, false),
      Seq(1.1, 2.2),
      Seq(Date.valueOf("2023-11-22"), Date.valueOf("2023-11-23")),
      Seq(LocalDateTime.of(2023, 11, 22, 11, 11, 11), LocalDateTime.of(2023, 11, 23, 12, 12, 12)),
      Seq(Timestamp.valueOf("2023-11-22 11:11:11.11"), Timestamp.valueOf("2023-11-23 12:12:12.12"))
    )

    val data = Seq(row).toDF(colNames: _*)
    val expectedNode = colNames.zip(row.productIterator.toSeq).toMap

    val schema = StructType(data.schema.map { sf =>
      sf.name match {
        case "localDateTimeArray" =>
          StructField(sf.name, DataTypes.createArrayType(DataTypes.TimestampNTZType, false), sf.nullable)
        case "zonedDateTimeArray" =>
          StructField(sf.name, DataTypes.createArrayType(DataTypes.TimestampType, false), sf.nullable)
        case "stringArray" => StructField(sf.name, DataTypes.createArrayType(DataTypes.StringType, false), sf.nullable)
        case "dateArray"   => StructField(sf.name, DataTypes.createArrayType(DataTypes.DateType, false), sf.nullable)
        case "string"      => StructField(sf.name, DataTypes.StringType, false)
        case _             => sf
      }
    })

    val df = ss.createDataFrame(data.rdd, schema)
    (expectedNode, df)
  }

  @Test
  def shouldApplySchemaForRelationshipsAndNodes(): Unit = {
    Assume.assumeTrue(
      "Skipping: requires Neo4j Enterprise Edition",
      TestUtil.isEnterpriseEdition(SparkConnectorScalaSuiteIT.session())
    )
    val expectedMap = createDatasetForRelationships(
      Map(
        Neo4jOptions.SCHEMA_OPTIMIZATION -> schemaOptimization
      )
    )

    val count: Long = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH p = (:NodeA)-[:MY_REL]->(:NodeB)
        |RETURN count(p)
        |""".stripMargin
    )
      .single()
      .get(0)
      .asLong()
    assertEquals(1L, count)

    val expected = Seq(
      Map(
        "name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeA-id",
        "propertyType" -> null,
        "entityType" -> "NODE",
        "type" -> "NODE_PROPERTY_EXISTENCE",
        "properties" -> Seq("id"),
        "labelsOrTypes" -> Seq("NodeA")
      ),
      Map(
        "name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeB-id",
        "propertyType" -> null,
        "entityType" -> "NODE",
        "type" -> "NODE_PROPERTY_EXISTENCE",
        "properties" -> Seq("id"),
        "labelsOrTypes" -> Seq("NodeB")
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeA-id",
        "propertyType" -> "STRING",
        "entityType" -> "NODE",
        "type" -> "NODE_PROPERTY_TYPE",
        "properties" -> Seq("id"),
        "labelsOrTypes" -> Seq("NodeA")
      ),
      Map(
        "name" -> "spark_NODE-TYPE-CONSTRAINT-NodeB-id",
        "propertyType" -> "STRING",
        "entityType" -> "NODE",
        "type" -> "NODE_PROPERTY_TYPE",
        "properties" -> Seq("id"),
        "labelsOrTypes" -> Seq("NodeB")
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-NOT_NULL-CONSTRAINT-MY_REL-boolean",
        "propertyType" -> null,
        "entityType" -> "RELATIONSHIP",
        "type" -> "RELATIONSHIP_PROPERTY_EXISTENCE",
        "properties" -> Seq("boolean"),
        "labelsOrTypes" -> Seq("MY_REL")
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-NOT_NULL-CONSTRAINT-MY_REL-float",
        "propertyType" -> null,
        "entityType" -> "RELATIONSHIP",
        "type" -> "RELATIONSHIP_PROPERTY_EXISTENCE",
        "properties" -> Seq("float"),
        "labelsOrTypes" -> Seq("MY_REL")
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-NOT_NULL-CONSTRAINT-MY_REL-int",
        "propertyType" -> null,
        "entityType" -> "RELATIONSHIP",
        "type" -> "RELATIONSHIP_PROPERTY_EXISTENCE",
        "properties" -> Seq("int"),
        "labelsOrTypes" -> Seq("MY_REL")
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-boolean",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("boolean"),
        "propertyType" -> "BOOLEAN"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-booleanArray",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("booleanArray"),
        "propertyType" -> "LIST<BOOLEAN NOT NULL>"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-date",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("date"),
        "propertyType" -> "DATE"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-dateArray",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("dateArray"),
        "propertyType" -> "LIST<DATE NOT NULL>"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-float",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("float"),
        "propertyType" -> "FLOAT"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-floatArray",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("floatArray"),
        "propertyType" -> "LIST<FLOAT NOT NULL>"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-int",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("int"),
        "propertyType" -> "INTEGER"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-intArray",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("intArray"),
        "propertyType" -> "LIST<INTEGER NOT NULL>"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-localDateTime",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("localDateTime"),
        "propertyType" -> "LOCAL DATETIME"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-localDateTimeArray",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("localDateTimeArray"),
        "propertyType" -> "LIST<LOCAL DATETIME NOT NULL>"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-string",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("string"),
        "propertyType" -> "STRING"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-stringArray",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("stringArray"),
        "propertyType" -> "LIST<STRING NOT NULL>"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-zonedDateTime",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("zonedDateTime"),
        "propertyType" -> "ZONED DATETIME"
      ),
      Map(
        "name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-zonedDateTimeArray",
        "type" -> "RELATIONSHIP_PROPERTY_TYPE",
        "entityType" -> "RELATIONSHIP",
        "labelsOrTypes" -> Seq("MY_REL"),
        "properties" -> Seq("zonedDateTimeArray"),
        "propertyType" -> "LIST<ZONED DATETIME NOT NULL>"
      )
    )

    val keys = Seq("name", "type", "entityType", "labelsOrTypes", "properties", "propertyType")
    val actual = SparkConnectorScalaSuiteIT.session()
      .run(s"SHOW CONSTRAINTS YIELD ${keys.mkString(", ")} RETURN * ORDER BY name")
      .list()
      .asScala
      .map(r => keys.map(key => (key, mapData(r.get(key).asObject()))).toMap)
      .toSeq

    assertEquals(expected, actual)

    val actualMap = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (s:NodeA)-[r:MY_REL]->(t:NodeB)
        |RETURN s.id AS idSource, t.id AS idTarget, r
        |""".stripMargin
    )
      .list()
      .asScala
      .map(r =>
        Map("idSource" -> r.get("idSource").asString(), "idTarget" -> r.get("idTarget").asString()) ++ r.get(
          "r"
        ).asRelationship().asMap().asScala
      )
      .head
      .mapValues(mapData)
      .toMap

    assertEquals(expectedMap, actualMap)
  }

  private def createDatasetForRelationships(options: Map[String, String]) = {
    SparkConnectorScalaSuiteIT.session()
      .run("CREATE (:NodeA{id: 'a'}), (:NodeB{id: 'b'})")
      .consume()
    val colNames = Array(
      "idSource",
      "idTarget",
      "string",
      "int",
      "boolean",
      "float",
      "date",
      "localDateTime",
      "zonedDateTime",
      "stringArray",
      "intArray",
      "booleanArray",
      "floatArray",
      "dateArray",
      "localDateTimeArray",
      "zonedDateTimeArray"
    )
    val row = (
      "a",
      "b",
      "Foo",
      1,
      false,
      1.1,
      Date.valueOf("2023-11-22"),
      LocalDateTime.of(2023, 11, 22, 12, 12, 12),
      Timestamp.valueOf(s"2020-11-22 11:11:11.11"),
      Seq("Foo1", "Foo2"),
      Seq(1, 2),
      Seq(true, false),
      Seq(1.1, 2.2),
      Seq(Date.valueOf("2023-11-22"), Date.valueOf("2023-11-23")),
      Seq(LocalDateTime.of(2023, 11, 22, 11, 11, 11), LocalDateTime.of(2023, 11, 23, 12, 12, 12)),
      Seq(Timestamp.valueOf("2023-11-22 11:11:11.11"), Timestamp.valueOf("2023-11-23 12:12:12.12"))
    )
    val data = Seq(row).toDF(colNames: _*)

    val schema = StructType(data.schema.map { sf =>
      sf.name match {
        case "localDateTimeArray" =>
          StructField(sf.name, DataTypes.createArrayType(DataTypes.TimestampNTZType, false), sf.nullable)
        case "zonedDateTimeArray" =>
          StructField(sf.name, DataTypes.createArrayType(DataTypes.TimestampType, false), sf.nullable)
        case "stringArray" => StructField(sf.name, DataTypes.createArrayType(DataTypes.StringType, false), sf.nullable)
        case "dateArray"   => StructField(sf.name, DataTypes.createArrayType(DataTypes.DateType, false), sf.nullable)
        case "idSource"    => StructField(sf.name, DataTypes.StringType, false)
        case "idTarget"    => StructField(sf.name, DataTypes.StringType, false)
        case _             => sf
      }
    })

    ss.createDataFrame(data.rdd, schema)
      .write
      .mode(SaveMode.Overwrite)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "MY_REL")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.labels", ":NodeA")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.source.node.keys", "idSource:id")
      .option("relationship.target.labels", ":NodeB")
      .option("relationship.target.node.keys", "idTarget:id")
      .option("relationship.target.save.mode", "Overwrite")
      .options(options)
      .save()

    colNames.zip(row.productIterator.toSeq).toMap
  }

  @Test
  def shouldApplyUniqueConstraintForNode(): Unit = {
    val total = 10
    val ds = (1 to total)
      .map(i => i.toString)
      .toDF("surname")

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":Person:Customer")
      .option("node.keys", "surname")
      .option(Neo4jOptions.SCHEMA_OPTIMIZATION_NODE_KEY, ConstraintsOptimizationType.UNIQUE.toString)
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

    val actualConstraint = SparkConnectorScalaSuiteIT.session().run(NODE_UNIQUENESS_SHOW_CONSTRAINTS_QUERY)
      .list()
      .asScala
      .map(_.asMap(v => v.asObject()).asScala.mapValues(mapData).filter(k => k._1 != "id").toMap)
      .head
    val expectedConstraint = Map(
      "name" -> "spark_NODE_UNIQUE-CONSTRAINT_Person_surname",
      "type" -> "NODE_PROPERTY_UNIQUENESS",
      "entityType" -> "NODE",
      "labelsOrTypes" -> Seq("Person"),
      "properties" -> Seq("surname"),
      "ownedIndex" -> "spark_NODE_UNIQUE-CONSTRAINT_Person_surname"
    )
    assertEquals(expectedConstraint, actualConstraint)

    SparkConnectorScalaSuiteIT.session().run("DROP CONSTRAINT `spark_NODE_UNIQUE-CONSTRAINT_Person_surname`").consume()
  }

  @Test
  def shouldApplyNodeKeyConstraintForNode(): Unit = {
    Assume.assumeTrue(
      "Skipping: requires Neo4j Enterprise Edition",
      TestUtil.isEnterpriseEdition(SparkConnectorScalaSuiteIT.session())
    )
    val total = 10
    val ds = (1 to total)
      .map(i => i.toString)
      .toDF("surname")

    ds.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":Person:Customer")
      .option("node.keys", "surname")
      .option(Neo4jOptions.SCHEMA_OPTIMIZATION_NODE_KEY, ConstraintsOptimizationType.KEY.toString)
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

    val actualConstraint = SparkConnectorScalaSuiteIT.session().run(SHOW_CONSTRAINTS_QUERY)
      .list()
      .asScala
      .map(_.asMap(v => v.asObject()).asScala.mapValues(mapData).filter(k => k._1 != "id").toMap)
      .head
    val expectedConstraint = Map(
      "name" -> "spark_NODE_KEY-CONSTRAINT_Person_surname",
      "type" -> "NODE_KEY",
      "entityType" -> "NODE",
      "labelsOrTypes" -> Seq("Person"),
      "properties" -> Seq("surname"),
      "ownedIndex" -> "spark_NODE_KEY-CONSTRAINT_Person_surname"
    )
    assertEquals(expectedConstraint, actualConstraint)

    SparkConnectorScalaSuiteIT.session().run("DROP CONSTRAINT `spark_NODE_KEY-CONSTRAINT_Person_surname`").consume()
  }

  @Test
  def shouldApplyUniqueConstraintForRelationship(): Unit = {
    val expectedMap = createDatasetForRelationships(
      Map(
        Neo4jOptions.SCHEMA_OPTIMIZATION_RELATIONSHIP_KEY -> ConstraintsOptimizationType.UNIQUE.toString,
        "relationship.keys" -> "string,int"
      )
    )
    val actualMap = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (s:NodeA)-[r:MY_REL]->(t:NodeB)
        |RETURN s.id AS idSource, t.id AS idTarget, r
        |""".stripMargin
    )
      .list()
      .asScala
      .map(r =>
        Map("idSource" -> r.get("idSource").asString(), "idTarget" -> r.get("idTarget").asString()) ++ r.get(
          "r"
        ).asRelationship().asMap().asScala
      )
      .head
      .mapValues(mapData)
      .toMap

    assertEquals(expectedMap, actualMap)

    val actualConstraint = SparkConnectorScalaSuiteIT.session().run(RELATIONSHIP_UNIQUENESS_SHOW_CONSTRAINTS_QUERY)
      .list()
      .asScala
      .map(_.asMap(v => v.asObject()).asScala.mapValues(mapData).filter(k => k._1 != "id").toMap)
      .head
    val expectedConstraint = Map(
      "name" -> "spark_RELATIONSHIP_UNIQUE-CONSTRAINT_MY_REL_string-int",
      "type" -> "RELATIONSHIP_PROPERTY_UNIQUENESS",
      "entityType" -> "RELATIONSHIP",
      "labelsOrTypes" -> Seq("MY_REL"),
      "properties" -> Seq("string", "int"),
      "ownedIndex" -> "spark_RELATIONSHIP_UNIQUE-CONSTRAINT_MY_REL_string-int"
    )
    assertEquals(expectedConstraint, actualConstraint)
  }

  @Test
  def shouldApplyRelUniqueConstraintForRelationship(): Unit = {
    Assume.assumeTrue(
      "Skipping: requires Neo4j Enterprise Edition",
      TestUtil.isEnterpriseEdition(SparkConnectorScalaSuiteIT.session())
    )
    val expectedMap = createDatasetForRelationships(
      Map(
        Neo4jOptions.SCHEMA_OPTIMIZATION_RELATIONSHIP_KEY -> ConstraintsOptimizationType.KEY.toString,
        "relationship.keys" -> "string,int"
      )
    )

    val actualMap = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (s:NodeA)-[r:MY_REL]->(t:NodeB)
        |RETURN s.id AS idSource, t.id AS idTarget, r
        |""".stripMargin
    )
      .list()
      .asScala
      .map(r =>
        Map("idSource" -> r.get("idSource").asString(), "idTarget" -> r.get("idTarget").asString()) ++ r.get(
          "r"
        ).asRelationship().asMap().asScala
      )
      .head
      .mapValues(mapData)
      .toMap

    assertEquals(expectedMap, actualMap)

    val actualConstraint = SparkConnectorScalaSuiteIT.session().run(SHOW_CONSTRAINTS_QUERY)
      .list()
      .asScala
      .map(_.asMap(v => v.asObject()).asScala.mapValues(mapData).filter(k => k._1 != "id").toMap)
      .head
    val expectedConstraint = Map(
      "name" -> "spark_RELATIONSHIP_KEY-CONSTRAINT_MY_REL_string-int",
      "type" -> "RELATIONSHIP_KEY",
      "entityType" -> "RELATIONSHIP",
      "labelsOrTypes" -> Seq("MY_REL"),
      "properties" -> Seq("string", "int"),
      "ownedIndex" -> "spark_RELATIONSHIP_KEY-CONSTRAINT_MY_REL_string-int"
    )

    assertEquals(expectedConstraint, actualConstraint)
  }

  @Test
  def shouldWriteNodeWithLegacyTypeConversionDisabledByDefault(): Unit = {
    val df = sparkSession.sql(
      """
        |SELECT
        |  'legacy-type-conversion' AS id,
        |  timestamp('2025-01-01 11:11:11') AS timestamp,
        |  CAST('2025-01-01 11:11:11' AS TIMESTAMP_NTZ) AS timestampNtz,
        |  INTERVAL '4' DAY AS dayInterval,
        |  INTERVAL '10 05' DAY TO HOUR AS dayToHour,
        |  timestamp('2025-01-02 18:30:00.454') - timestamp('2024-01-01 00:00:00') AS arithmeticDuration,
        |  INTERVAL '3' YEAR AS yearInterval,
        |  INTERVAL '1-2' YEAR TO MONTH AS yearToMonth,
        |  CAST('erik' AS BINARY) AS binary,
        |  CAST(array(1, 2, 3) AS array<tinyint>) AS byteArray
        |""".stripMargin
    )
    assertTrue(df.schema("dayInterval").dataType.isInstanceOf[DayTimeIntervalType])
    assertTrue(df.schema("dayToHour").dataType.isInstanceOf[DayTimeIntervalType])
    assertTrue(df.schema("arithmeticDuration").dataType.isInstanceOf[DayTimeIntervalType])
    assertTrue(df.schema("yearInterval").dataType.isInstanceOf[YearMonthIntervalType])
    assertTrue(df.schema("yearToMonth").dataType.isInstanceOf[YearMonthIntervalType])

    df.write
      .mode(SaveMode.Overwrite)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":LegacyTypeConversionDisabled")
      .option("node.keys", "id")
      .save()

    val actual = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (n:LegacyTypeConversionDisabled {id: 'legacy-type-conversion'})
        |RETURN n
        |""".stripMargin
    )
      .single()
      .get("n")
      .asNode()
      .asMap()
      .asScala
      .toMap
    assertEquals(
      Set(
        "id",
        "timestamp",
        "timestampNtz",
        "dayInterval",
        "dayToHour",
        "arithmeticDuration",
        "yearInterval",
        "yearToMonth",
        "binary",
        "byteArray"
      ),
      actual.keySet
    )
    assertEquals("legacy-type-conversion", actual("id"))
    val expectedZoned = ZonedDateTime.of(2025, 1, 1, 11, 11, 11, 0, ZoneOffset.UTC)
    assertEquals(expectedZoned, actual("timestamp"))
    val expectedNtz = LocalDateTime.of(2025, 1, 1, 11, 11, 11)
    assertEquals(expectedNtz, actual("timestampNtz"))
    val dayInterval = actual("dayInterval").asInstanceOf[IsoDuration]
    assertEquals(0L, dayInterval.months())
    assertEquals(4L, dayInterval.days())
    assertEquals(0L, dayInterval.seconds())
    assertEquals(0, dayInterval.nanoseconds())
    val dayToHour = actual("dayToHour").asInstanceOf[IsoDuration]
    assertEquals(0L, dayToHour.months())
    assertEquals(10L, dayToHour.days())
    assertEquals(18000L, dayToHour.seconds())
    assertEquals(0, dayToHour.nanoseconds())
    val arithmetic = actual("arithmeticDuration").asInstanceOf[IsoDuration]
    assertEquals(0L, arithmetic.months())
    assertEquals(367L, arithmetic.days())
    assertEquals(66600L, arithmetic.seconds())
    assertEquals(454000000, arithmetic.nanoseconds())
    val yearInterval = actual("yearInterval").asInstanceOf[IsoDuration]
    assertEquals(36L, yearInterval.months())
    assertEquals(0L, yearInterval.days())
    assertEquals(0L, yearInterval.seconds())
    assertEquals(0, yearInterval.nanoseconds())
    val yearToMonth = actual("yearToMonth").asInstanceOf[IsoDuration]
    assertEquals(14L, yearToMonth.months())
    assertEquals(0L, yearToMonth.days())
    assertEquals(0L, yearToMonth.seconds())
    assertEquals(0, yearToMonth.nanoseconds())
    assertArrayEquals(Array[Byte](101, 114, 105, 107), actual("binary").asInstanceOf[Array[Byte]])
    assertArrayEquals(Array[Byte](1, 2, 3), actual("byteArray").asInstanceOf[Array[Byte]])
  }

  @Test
  def shouldWriteNodeWithLegacyTypeConversionEnabled(): Unit = {
    val df = sparkSession.sql(
      """
        |SELECT
        |  'legacy-type-conversion' AS id,
        |  timestamp('2025-01-01 11:11:11') AS timestamp,
        |  CAST('2025-01-01 11:11:11' AS TIMESTAMP_NTZ) AS timestampNtz,
        |  INTERVAL '4' DAY AS dayInterval,
        |  INTERVAL '10 05' DAY TO HOUR AS dayToHour,
        |  timestamp('2025-01-02 18:30:00.454') - timestamp('2024-01-01 00:00:00') AS arithmeticDuration,
        |  INTERVAL '3' YEAR AS yearInterval,
        |  INTERVAL '1-2' YEAR TO MONTH AS yearToMonth,
        |  CAST('erik' AS BINARY) AS binary,
        |  CAST(array(1, 2, 3) AS array<tinyint>) AS byteArray
        |""".stripMargin
    )
    assertTrue(df.schema("dayInterval").dataType.isInstanceOf[DayTimeIntervalType])
    assertTrue(df.schema("dayToHour").dataType.isInstanceOf[DayTimeIntervalType])
    assertTrue(df.schema("arithmeticDuration").dataType.isInstanceOf[DayTimeIntervalType])
    assertTrue(df.schema("yearInterval").dataType.isInstanceOf[YearMonthIntervalType])
    assertTrue(df.schema("yearToMonth").dataType.isInstanceOf[YearMonthIntervalType])

    df.write
      .mode(SaveMode.Overwrite)
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":LegacyTypeConversionEnabled")
      .option("node.keys", "id")
      .option(Neo4jOptions.TYPE_CONVERSION, "legacy")
      .save()

    val actual = SparkConnectorScalaSuiteIT.session().run(
      """
        |MATCH (n:LegacyTypeConversionEnabled {id: 'legacy-type-conversion'})
        |RETURN n
        |""".stripMargin
    )
      .single()
      .get("n")
      .asNode()
      .asMap()
      .asScala
      .toMap
    assertEquals(
      Set(
        "id",
        "timestamp",
        "timestampNtz",
        "dayInterval",
        "dayToHour",
        "arithmeticDuration",
        "yearInterval",
        "yearToMonth",
        "binary",
        "byteArray"
      ),
      actual.keySet
    )
    assertEquals("legacy-type-conversion", actual("id"))
    val expectedTimestamp = ZonedDateTime.of(2025, 1, 1, 11, 11, 11, 0, ZoneOffset.UTC)
      .withZoneSameInstant(ZoneId.systemDefault())
      .toLocalDateTime
    assertEquals(expectedTimestamp, actual("timestamp"))
    assertEquals(
      DateTimeUtils.localDateTimeToMicros(LocalDateTime.of(2025, 1, 1, 11, 11, 11)),
      actual("timestampNtz").asInstanceOf[java.lang.Number].longValue()
    )
    assertEquals(4L * 24L * 3600L * 1000000L, actual("dayInterval").asInstanceOf[java.lang.Number].longValue())
    assertEquals((10L * 24L + 5L) * 3600L * 1000000L, actual("dayToHour").asInstanceOf[java.lang.Number].longValue())
    assertEquals(
      (367L * 24L * 3600L + 66600L) * 1000000L + 454000L,
      actual("arithmeticDuration").asInstanceOf[java.lang.Number].longValue()
    )
    assertEquals(36L, actual("yearInterval").asInstanceOf[java.lang.Number].longValue())
    assertEquals(14L, actual("yearToMonth").asInstanceOf[java.lang.Number].longValue())
    assertArrayEquals(Array[Byte](101, 114, 105, 107), actual("binary").asInstanceOf[Array[Byte]])
    val byteArray = actual("byteArray").asInstanceOf[java.util.List[_]].asScala
      .map(_.asInstanceOf[java.lang.Number].longValue())
    assertEquals(Seq(1L, 2L, 3L), byteArray)
  }
}
