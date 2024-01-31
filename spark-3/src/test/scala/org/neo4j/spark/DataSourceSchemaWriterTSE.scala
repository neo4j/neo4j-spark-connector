package org.neo4j.spark

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.{Assume, BeforeClass, Test}
import org.neo4j.spark.util.{ConstraintsOptimizationType, Neo4jOptions, SchemaConstraintsOptimizationType}

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, LocalDateTime}
import scala.collection.JavaConverters.{iterableAsScalaIterableConverter, mapAsScalaMapConverter}

object DataSourceSchemaWriterTSE {
  @BeforeClass
  def checkNeo4jVersion() {
    Assume.assumeTrue(TestUtil.neo4jVersionAsDouble() >= 5.13)
  }
}

class DataSourceSchemaWriterTSE extends SparkConnectorScalaBaseTSE {
  val sparkSession = SparkSession.builder().getOrCreate()

  import sparkSession.implicits._

  private def mapData(x: Any): Any = x match {
    case null => null
    case a: Array[_] => a.toSeq.map(mapData)
    case l: java.util.List[_] => l.asScala.toSeq.map(mapData)
    case d: LocalDate => Date.valueOf(d)
    case ldt: LocalDateTime => Timestamp.valueOf(ldt)
    case any: Any => any
  }

  private val schemaOptimization = SchemaConstraintsOptimizationType.values
    .filterNot(_ == SchemaConstraintsOptimizationType.NONE)
    .mkString(",")

  @Test
  def shouldApplySchemaForNodes(): Unit = {
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
        |""".stripMargin)
      .single()
      .get(0)
      .asLong()
    assertEquals(1L, count)

    val expectedSchema = Seq(
      Map("name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-boolean", "type" -> "NODE_PROPERTY_EXISTENCE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("boolean"), "propertyType" -> null),
      Map("name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-float", "type" -> "NODE_PROPERTY_EXISTENCE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("float"), "propertyType" -> null),
      Map("name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-int", "type" -> "NODE_PROPERTY_EXISTENCE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("int"), "propertyType" -> null),
      Map("name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-string", "type" -> "NODE_PROPERTY_EXISTENCE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("string"), "propertyType" -> null),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-boolean", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("boolean"), "propertyType" -> "BOOLEAN"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-booleanArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("booleanArray"), "propertyType" -> "LIST<BOOLEAN NOT NULL>"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-date", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("date"), "propertyType" -> "DATE"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-dateArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("dateArray"), "propertyType" -> "LIST<DATE NOT NULL>"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-float", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("float"), "propertyType" -> "FLOAT"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-floatArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("floatArray"), "propertyType" -> "LIST<FLOAT NOT NULL>"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-int", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("int"), "propertyType" -> "INTEGER"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-intArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("intArray"), "propertyType" -> "LIST<INTEGER NOT NULL>"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-string", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("string"), "propertyType" -> "STRING"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-stringArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("stringArray"), "propertyType" -> "LIST<STRING NOT NULL>"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-timestamp", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("timestamp"), "propertyType" -> "LOCAL DATETIME"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-timestampArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("timestampArray"), "propertyType" -> "LIST<LOCAL DATETIME NOT NULL>")
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
      .readTransaction(tx => tx.run("MATCH (n:NodeWithSchema) RETURN n")
        .list()
        .asScala
        .map(_.get("n").asNode())
        .map(_.asMap()))
      .head
      .asScala
      .mapValues(mapData)
      .toMap

    assertEquals(expectedNode, actualNode)
  }

  @Test
  def shouldApplySchemaAndNodeKeysForNodes(): Unit = {
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
          |""".stripMargin)
      .single()
      .get(0)
      .asLong()
    assertEquals(1L, count)

    val expectedSchema = Seq(
      Map("name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-boolean", "type" -> "NODE_PROPERTY_EXISTENCE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("boolean"), "propertyType" -> null),
      Map("name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-float", "type" -> "NODE_PROPERTY_EXISTENCE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("float"), "propertyType" -> null),
      Map("name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-int", "type" -> "NODE_PROPERTY_EXISTENCE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("int"), "propertyType" -> null),
      Map("name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeWithSchema-string", "type" -> "NODE_PROPERTY_EXISTENCE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("string"), "propertyType" -> null),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-boolean", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("boolean"), "propertyType" -> "BOOLEAN"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-booleanArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("booleanArray"), "propertyType" -> "LIST<BOOLEAN NOT NULL>"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-date", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("date"), "propertyType" -> "DATE"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-dateArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("dateArray"), "propertyType" -> "LIST<DATE NOT NULL>"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-float", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("float"), "propertyType" -> "FLOAT"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-floatArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("floatArray"), "propertyType" -> "LIST<FLOAT NOT NULL>"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-int", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("int"), "propertyType" -> "INTEGER"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-intArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("intArray"), "propertyType" -> "LIST<INTEGER NOT NULL>"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-string", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("string"), "propertyType" -> "STRING"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-stringArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("stringArray"), "propertyType" -> "LIST<STRING NOT NULL>"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-timestamp", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("timestamp"), "propertyType" -> "LOCAL DATETIME"),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeWithSchema-timestampArray", "type" -> "NODE_PROPERTY_TYPE", "entityType" -> "NODE", "labelsOrTypes" -> Seq("NodeWithSchema"), "properties" -> Seq("timestampArray"), "propertyType" -> "LIST<LOCAL DATETIME NOT NULL>"),
        Map("name" -> "spark_NODE_KEY-CONSTRAINT_NodeWithSchema_int-string", "propertyType" -> null, "properties" -> Seq("int", "string"), "labelsOrTypes" -> Seq("NodeWithSchema"), "entityType" -> "NODE", "type" -> "NODE_KEY")
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
      .readTransaction(tx => tx.run("MATCH (n:NodeWithSchema) RETURN n")
        .list()
        .asScala
        .map(_.get("n").asNode())
        .map(_.asMap()))
      .head
      .asScala
      .mapValues(mapData)
      .toMap

    assertEquals(expectedNode, actualNode)
  }

  private def createNodesDataFrameWithNotNullColumns: (Map[String, Any], DataFrame) = {
    val colNames = Array("string", "int", "boolean", "float", "date", "timestamp",
      "stringArray", "intArray", "booleanArray", "floatArray", "dateArray", "timestampArray")
    val row = (
      "Foo", 1, false, 1.1, Date.valueOf("2023-11-22"), Timestamp.valueOf(s"2020-11-22 11:11:11.11"),
      Seq("Foo1", "Foo2"),
      Seq(1, 2),
      Seq(true, false),
      Seq(1.1, 2.2),
      Seq(Date.valueOf("2023-11-22"), Date.valueOf("2023-11-23")),
      Seq(Timestamp.valueOf("2023-11-22 11:11:11.11"), Timestamp.valueOf("2023-11-23 12:12:12.12"))
    )
    val data = Seq(row).toDF(colNames: _*)

    val expectedNode = colNames.zip(row.productIterator.toSeq).toMap

    val schema = StructType(data.schema.map { sf =>
      sf.name match {
        case "timestampArray" => StructField(sf.name, DataTypes.createArrayType(DataTypes.TimestampType, false), sf.nullable)
        case "stringArray" => StructField(sf.name, DataTypes.createArrayType(DataTypes.StringType, false), sf.nullable)
        case "dateArray" => StructField(sf.name, DataTypes.createArrayType(DataTypes.DateType, false), sf.nullable)
        case "string" => StructField(sf.name, DataTypes.StringType, false)
        case _ => sf
      }
    })
    val df = ss.createDataFrame(data.rdd, schema)
    (expectedNode, df)
  }

  @Test
  def shouldApplySchemaForRelationshipsAndNodes(): Unit = {
    val expectedMap = createDatasetForRelationships(
      Map(
        Neo4jOptions.SCHEMA_OPTIMIZATION -> schemaOptimization
      )
    )

    val count: Long = SparkConnectorScalaSuiteIT.session().run(
        """
          |MATCH p = (:NodeA)-[:MY_REL]->(:NodeB)
          |RETURN count(p)
          |""".stripMargin)
      .single()
      .get(0)
      .asLong()
    assertEquals(1L, count)

    val expected = Seq(
      Map("name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeA-id", "propertyType" -> null, "entityType" -> "NODE", "type" -> "NODE_PROPERTY_EXISTENCE", "properties" -> Seq("id"), "labelsOrTypes" -> Seq("NodeA")),
      Map("name" -> "spark_NODE-NOT_NULL-CONSTRAINT-NodeB-id", "propertyType" -> null, "entityType" -> "NODE", "type" -> "NODE_PROPERTY_EXISTENCE", "properties" -> Seq("id"), "labelsOrTypes" -> Seq("NodeB")),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeA-id", "propertyType" -> "STRING", "entityType" -> "NODE", "type" -> "NODE_PROPERTY_TYPE", "properties" -> Seq("id"), "labelsOrTypes" -> Seq("NodeA")),
      Map("name" -> "spark_NODE-TYPE-CONSTRAINT-NodeB-id", "propertyType" -> "STRING", "entityType" -> "NODE", "type" -> "NODE_PROPERTY_TYPE", "properties" -> Seq("id"), "labelsOrTypes" -> Seq("NodeB")),
      Map("name" -> "spark_RELATIONSHIP-NOT_NULL-CONSTRAINT-MY_REL-boolean", "propertyType" -> null, "entityType" -> "RELATIONSHIP", "type" -> "RELATIONSHIP_PROPERTY_EXISTENCE", "properties" -> Seq("boolean"), "labelsOrTypes" -> Seq("MY_REL")),
      Map("name" -> "spark_RELATIONSHIP-NOT_NULL-CONSTRAINT-MY_REL-float", "propertyType" -> null, "entityType" -> "RELATIONSHIP", "type" -> "RELATIONSHIP_PROPERTY_EXISTENCE", "properties" -> Seq("float"), "labelsOrTypes" -> Seq("MY_REL")),
      Map("name" -> "spark_RELATIONSHIP-NOT_NULL-CONSTRAINT-MY_REL-int", "propertyType" -> null, "entityType" -> "RELATIONSHIP", "type" -> "RELATIONSHIP_PROPERTY_EXISTENCE", "properties" -> Seq("int"), "labelsOrTypes" -> Seq("MY_REL")),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-boolean", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("boolean"), "propertyType" -> "BOOLEAN"),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-booleanArray", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("booleanArray"), "propertyType" -> "LIST<BOOLEAN NOT NULL>"),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-date", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("date"), "propertyType" -> "DATE"),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-dateArray", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("dateArray"), "propertyType" -> "LIST<DATE NOT NULL>"),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-float", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("float"), "propertyType" -> "FLOAT"),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-floatArray", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("floatArray"), "propertyType" -> "LIST<FLOAT NOT NULL>"),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-int", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("int"), "propertyType" -> "INTEGER"),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-intArray", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("intArray"), "propertyType" -> "LIST<INTEGER NOT NULL>"),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-string", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("string"), "propertyType" -> "STRING"),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-stringArray", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("stringArray"), "propertyType" -> "LIST<STRING NOT NULL>"),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-timestamp", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("timestamp"), "propertyType" -> "LOCAL DATETIME"),
      Map("name" -> "spark_RELATIONSHIP-TYPE-CONSTRAINT-MY_REL-timestampArray", "type" -> "RELATIONSHIP_PROPERTY_TYPE", "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("timestampArray"), "propertyType" -> "LIST<LOCAL DATETIME NOT NULL>")
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
          |""".stripMargin)
      .list()
      .asScala
      .map(r => Map("idSource" -> r.get("idSource").asString(),
        "idTarget" -> r.get("idTarget").asString()) ++ r.get("r").asRelationship().asMap().asScala)
      .head
      .mapValues(mapData)
      .toMap

    assertEquals(expectedMap, actualMap)
  }

  private def createDatasetForRelationships(options: Map[String, String]) = {
    SparkConnectorScalaSuiteIT.session()
      .run("CREATE (:NodeA{id: 'a'}), (:NodeB{id: 'b'})")
      .consume()
    val colNames = Array("idSource", "idTarget", "string", "int", "boolean", "float", "date", "timestamp",
      "stringArray", "intArray", "booleanArray", "floatArray", "dateArray", "timestampArray")
    val row = ("a", "b", "Foo", 1, false, 1.1, Date.valueOf("2023-11-22"), Timestamp.valueOf(s"2020-11-22 11:11:11.11"),
      Seq("Foo1", "Foo2"),
      Seq(1, 2),
      Seq(true, false),
      Seq(1.1, 2.2),
      Seq(Date.valueOf("2023-11-22"), Date.valueOf("2023-11-23")),
      Seq(Timestamp.valueOf("2023-11-22 11:11:11.11"), Timestamp.valueOf("2023-11-23 12:12:12.12")))
    val data = Seq(row).toDF(colNames: _*)

    val schema = StructType(data.schema.map { sf =>
      sf.name match {
        case "timestampArray" => StructField(sf.name, DataTypes.createArrayType(DataTypes.TimestampType, false), sf.nullable)
        case "stringArray" => StructField(sf.name, DataTypes.createArrayType(DataTypes.StringType, false), sf.nullable)
        case "dateArray" => StructField(sf.name, DataTypes.createArrayType(DataTypes.DateType, false), sf.nullable)
        case "idSource" => StructField(sf.name, DataTypes.StringType, false)
        case "idTarget" => StructField(sf.name, DataTypes.StringType, false)
        case _ => sf
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
          |""".stripMargin).list().asScala
      .map(r => r.asMap().asScala)
      .toSet
    val expected = ds.collect().map(row => Map("surname" -> row.getAs[String]("surname")))
      .toSet
    assertEquals(expected, records)

    val actualConstraint = SparkConnectorScalaSuiteIT.session().run("SHOW CONSTRAINTS")
      .list()
      .asScala
      .map(_.asMap(v => v.asObject()).asScala.mapValues(mapData).filter(k => k._1 != "id").toMap)
      .head
    val expectedConstraint = Map("name" -> "spark_NODE_UNIQUE-CONSTRAINT_Person_surname", "type" -> "UNIQUENESS",
      "entityType" -> "NODE", "labelsOrTypes" -> Seq("Person"), "properties" -> Seq("surname"), "ownedIndex" -> "spark_NODE_UNIQUE-CONSTRAINT_Person_surname",
      "propertyType" -> null)
    assertEquals(expectedConstraint, actualConstraint)

    SparkConnectorScalaSuiteIT.session().run("DROP CONSTRAINT `spark_NODE_UNIQUE-CONSTRAINT_Person_surname`").consume()
  }

  @Test
  def shouldApplyNodeKeyConstraintForNode(): Unit = {
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
          |""".stripMargin).list().asScala
      .map(r => r.asMap().asScala)
      .toSet
    val expected = ds.collect().map(row => Map("surname" -> row.getAs[String]("surname")))
      .toSet
    assertEquals(expected, records)

    val actualConstraint = SparkConnectorScalaSuiteIT.session().run("SHOW CONSTRAINTS")
      .list()
      .asScala
      .map(_.asMap(v => v.asObject()).asScala.mapValues(mapData).filter(k => k._1 != "id").toMap)
      .head
    val expectedConstraint = Map("name" -> "spark_NODE_KEY-CONSTRAINT_Person_surname", "type" -> "NODE_KEY",
      "entityType" -> "NODE", "labelsOrTypes" -> Seq("Person"), "properties" -> Seq("surname"), "ownedIndex" -> "spark_NODE_KEY-CONSTRAINT_Person_surname",
      "propertyType" -> null)
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
          |""".stripMargin)
      .list()
      .asScala
      .map(r => Map("idSource" -> r.get("idSource").asString(),
        "idTarget" -> r.get("idTarget").asString()) ++ r.get("r").asRelationship().asMap().asScala)
      .head
      .mapValues(mapData)
      .toMap
    assertEquals(expectedMap, actualMap)
    val actualConstraint = SparkConnectorScalaSuiteIT.session().run("SHOW CONSTRAINTS")
      .list()
      .asScala
      .map(_.asMap(v => v.asObject()).asScala.mapValues(mapData).filter(k => k._1 != "id").toMap)
      .head
    val expectedConstraint = Map("name" -> "spark_RELATIONSHIP_UNIQUE-CONSTRAINT_MY_REL_string-int", "type" -> "RELATIONSHIP_UNIQUENESS",
      "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("string", "int"), "ownedIndex" -> "spark_RELATIONSHIP_UNIQUE-CONSTRAINT_MY_REL_string-int",
      "propertyType" -> null)
    assertEquals(expectedConstraint, actualConstraint)
  }

  @Test
  def shouldApplyRelUniqueConstraintForRelationship(): Unit = {
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
          |""".stripMargin)
      .list()
      .asScala
      .map(r => Map("idSource" -> r.get("idSource").asString(),
        "idTarget" -> r.get("idTarget").asString()) ++ r.get("r").asRelationship().asMap().asScala)
      .head
      .mapValues(mapData)
      .toMap
    assertEquals(expectedMap, actualMap)
    val actualConstraint = SparkConnectorScalaSuiteIT.session().run("SHOW CONSTRAINTS")
      .list()
      .asScala
      .map(_.asMap(v => v.asObject()).asScala.mapValues(mapData).filter(k => k._1 != "id").toMap)
      .head
    val expectedConstraint = Map("name" -> "spark_RELATIONSHIP_KEY-CONSTRAINT_MY_REL_string-int", "type" -> "RELATIONSHIP_KEY",
      "entityType" -> "RELATIONSHIP", "labelsOrTypes" -> Seq("MY_REL"), "properties" -> Seq("string", "int"), "ownedIndex" -> "spark_RELATIONSHIP_KEY-CONSTRAINT_MY_REL_string-int",
      "propertyType" -> null)
    assertEquals(expectedConstraint, actualConstraint)
  }
}
