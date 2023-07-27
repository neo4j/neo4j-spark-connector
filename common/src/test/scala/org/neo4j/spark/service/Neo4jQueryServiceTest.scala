package org.neo4j.spark.service

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.connector.expressions.aggregate.{Count, Max, Min, Sum}
import org.apache.spark.sql.connector.expressions.{Expression, NullOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.sources._
import org.junit.Assert._
import org.junit.Test
import org.neo4j.spark.config.TopN
import org.neo4j.spark.util.Neo4jImplicits.CypherImplicits
import org.neo4j.spark.util.{DummyNamedReference, Neo4jOptions, QueryType}

import scala.collection.immutable.HashMap

class Neo4jQueryServiceTest {

  @Test
  def testNodeOneLabel(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy).createQuery()

    assertEquals("MATCH (n:`Person`) RETURN n", query)
  }

  @Test
  def testNodeMultipleLabels(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, ":Person:Player:Midfield")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy).createQuery()

    assertEquals("MATCH (n:`Person`:`Player`:`Midfield`) RETURN n", query)
  }

  @Test
  def testNodeMultipleLabelsWithPartitions(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, ":Person:Player:Midfield")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(partitionPagination = PartitionPagination(0, 0, TopN(100)))).createQuery()

    assertEquals("MATCH (n:`Person`:`Player`:`Midfield`) RETURN n LIMIT 100", query)
  }

  @Test
  def testNodeOneLabelWithOneSelectedColumn(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionPagination.EMPTY, Seq("name"))
    ).createQuery()

    assertEquals("MATCH (n:`Person`) RETURN n.name AS name", query)
  }

  @Test
  def testNodeOneLabelWithMultipleColumnSelected(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionPagination.EMPTY, List("name", "bornDate"))
    ).createQuery()

    assertEquals("MATCH (n:`Person`) RETURN n.name AS name, n.bornDate AS bornDate", query)
  }

  @Test
  def testNodeOneLabelWithInternalIdSelected(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionPagination.EMPTY, List("<id>"))
    ).createQuery()

    assertEquals("MATCH (n:`Person`) RETURN id(n) AS `<id>`", query)
  }

  @Test
  def testNodeFilterEqualTo(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualTo("name", "John Doe")
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    val paramName = "$" + "name".toParameterName("John Doe")

    assertEquals(s"MATCH (n:`Person`) WHERE n.name = $paramName RETURN n", query)
  }

  @Test
  def testNodeFilterEqualNullSafe(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualNullSafe("name", "John Doe"),
      EqualTo("age", 36)
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    val nameParameterName = "$" + "name".toParameterName("John Doe")
    val ageParameterName = "$" + "age".toParameterName(36)

    assertEquals(
      s"""MATCH (n:`Person`)
         | WHERE (((n.name IS NULL AND $nameParameterName IS NULL)
         | OR n.name = $nameParameterName) AND n.age = $ageParameterName)
         | RETURN n""".stripMargin.replaceAll("\n", ""), query)
  }

  @Test
  def testNodeFilterEqualNullSafeWithNullValue(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualNullSafe("name", null),
      EqualTo("age", 36)
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    val nameParameterName = "$" + "name".toParameterName(null)
    val ageParameterName = "$" + "age".toParameterName(36)

    assertEquals(s"MATCH (n:`Person`) WHERE (((n.name IS NULL AND $nameParameterName IS NULL) OR n.name = $nameParameterName) AND n.age = $ageParameterName) RETURN n", query)
  }

  @Test
  def testNodeFilterStartsEndsWith(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      StringStartsWith("name", "Person Name"),
      StringEndsWith("name", "Person Surname")
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    val nameOneParameterName = "$" + "name".toParameterName("Person Name")
    val nameTwoParameterName = "$" + "name".toParameterName("Person Surname")

    assertEquals(
      s"""MATCH (n:`Person`)
         | WHERE (n.name STARTS WITH $nameOneParameterName
         | AND n.name ENDS WITH $nameTwoParameterName)
         | RETURN n""".stripMargin.replaceAll("\n", ""), query)
  }

  @Test
  def testRelationshipWithOneColumnSelected(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array.empty[Filter],
      PartitionPagination.EMPTY,
      List("source.name")
    )).createQuery()

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`) " +
      "MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`", query)
  }

  @Test
  def testRelationshipWithMoreColumnSelected(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array.empty[Filter],
      PartitionPagination.EMPTY,
      List("source.name", "<source.id>")
    )).createQuery()

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`) " +
      "MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`, id(source) AS `<source.id>`", query)
  }

  @Test
  def testRelationshipWithMoreColumnSelectedWithPartitions(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array.empty[Filter],
      PartitionPagination(0, 0, TopN(limit = 100)),
      List("source.name", "<source.id>")
    )).createQuery()

    assertEquals(
      """MATCH (source:`Person`)
        |MATCH (target:`Person`)
        |MATCH (source)-[rel:`KNOWS`]->(target)
        |RETURN source.name AS `source.name`, id(source) AS `<source.id>`
        |LIMIT 100"""
        .stripMargin
        .replace(System.lineSeparator(), " "),
      query)
  }

  @Test
  def testRelationshipWithMoreColumnsSelected(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array.empty[Filter],
      PartitionPagination.EMPTY,
      List("source.name", "source.id", "rel.someprops", "target.date")
    )).createQuery()

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`) " +
      "MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`, source.id AS `source.id`, rel.someprops AS `rel.someprops`, target.date AS `target.date`", query)
  }

  @Test
  def testRelationshipFilterEqualTo(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualTo("source.name", "John Doe")
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    val parameterName = "$" + "source.name".toParameterName("John Doe")

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`) " +
      s"MATCH (source)-[rel:`KNOWS`]->(target) WHERE source.name = $parameterName RETURN rel, source AS source, target AS target", query)
  }

  @Test
  def testRelationshipFilterNotEqualTo(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      Or(EqualTo("source.name", "John Doe"), EqualTo("target.name", "John Doe"))
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    val paramOneName = "$" + "source.name".toParameterName("John Doe")
    val paramTwoName = "$" + "target.name".toParameterName("John Doe")

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`) " +
      s"MATCH (source)-[rel:`KNOWS`]->(target) WHERE (source.name = $paramOneName OR target.name = $paramTwoName) RETURN rel, source AS source, target AS target", query)
  }

  @Test
  def testRelationshipAndFilterEqualTo(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "true")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualTo("source.id", "14"),
      EqualTo("target.id", "16")
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    val sourceIdParameterName = "$" + "source.id".toParameterName(14)
    val targetIdParameterName = "$" + "target.id".toParameterName(16)

    assertEquals(
      s"""MATCH (source:`Person`)
         | MATCH (target:`Person`)
         | MATCH (source)-[rel:`KNOWS`]->(target)
         | WHERE (source.id = $sourceIdParameterName AND target.id = $targetIdParameterName)
         | RETURN rel, source AS source, target AS target
         |""".stripMargin.replaceAll("\n", ""), query)
  }

  @Test
  def testComplexNodeConditions(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      Or(EqualTo("name", "John Doe"), EqualTo("name", "John Scofield")),
      Or(EqualTo("age", 15), GreaterThanOrEqual("age", 18)),
      Or(Not(EqualTo("age", 22)), Not(LessThan("age", 11)))
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    val parameterNames: Map[String, String] = HashMap(
      "name_1" -> "$".concat("name".toParameterName("John Doe")),
      "name_2" -> "$".concat("name".toParameterName("John Scofield")),
      "age_1" -> "$".concat("age".toParameterName(15)),
      "age_2" -> "$".concat("age".toParameterName(18)),
      "age_3" -> "$".concat("age".toParameterName(22)),
      "age_4" -> "$".concat("age".toParameterName(11))
    )

    assertEquals(
      s"""MATCH (n:`Person`)
         | WHERE (((n.name = ${parameterNames("name_1")} OR n.name = ${parameterNames("name_2")})
         | AND (n.age = ${parameterNames("age_1")} OR n.age >= ${parameterNames("age_2")}))
         | AND (NOT (n.age = ${parameterNames("age_3")}) OR NOT (n.age < ${parameterNames("age_4")})))
         | RETURN n""".stripMargin.replaceAll("\n", ""), query)
  }

  @Test
  def testRelationshipFilterComplexConditionsNoMap(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person:Customer")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      Or(Or(EqualTo("source.name", "John Doe"), EqualTo("target.name", "John Doraemon")), EqualTo("source.name", "Jane Doe")),
      Or(EqualTo("target.age", 34), EqualTo("target.age", 18)),
      EqualTo("rel.score", 12)
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    val parameterNames = Map(
      "source.name_1" -> "$".concat("source.name".toParameterName("John Doe")),
      "target.name_1" -> "$".concat("target.name".toParameterName("John Doraemon")),
      "source.name_2" -> "$".concat("source.name".toParameterName("Jane Doe")),
      "target.age_1" -> "$".concat("target.age".toParameterName(34)),
      "target.age_2" -> "$".concat("target.age".toParameterName(18)),
      "rel.score" -> "$".concat("rel.score".toParameterName(12))
    )

    assertEquals(
      s"""MATCH (source:`Person`)
         | MATCH (target:`Person`:`Customer`)
         | MATCH (source)-[rel:`KNOWS`]->(target)
         | WHERE ((source.name = ${parameterNames("source.name_1")} OR target.name = ${parameterNames("target.name_1")} OR source.name = ${parameterNames("source.name_2")})
         | AND (target.age = ${parameterNames("target.age_1")} OR target.age = ${parameterNames("target.age_2")})
         | AND rel.score = ${parameterNames("rel.score")})
         | RETURN rel, source AS source, target AS target""".stripMargin.replaceAll("\n", ""), query)
  }

  @Test
  def testRelationshipFilterComplexConditionsWithMap(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "true")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person:Customer")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      Or(Or(EqualTo("source.name", "John Doe"), EqualTo("target.name", "John Doraemon")), EqualTo("source.name", "Jane Doe")),
      Or(EqualTo("target.age", 34), EqualTo("target.age", 18)),
      EqualTo("rel.score", 12)
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    val parameterNames = Map(
      "source.name_1" -> "$".concat("source.name".toParameterName("John Doe")),
      "target.name_1" -> "$".concat("target.name".toParameterName("John Doraemon")),
      "source.name_2" -> "$".concat("source.name".toParameterName("Jane Doe")),
      "target.age_1" -> "$".concat("target.age".toParameterName(34)),
      "target.age_2" -> "$".concat("target.age".toParameterName(18)),
      "rel.score" -> "$".concat("rel.score".toParameterName(12))
    )

    assertEquals(
      s"""MATCH (source:`Person`)
         | MATCH (target:`Person`:`Customer`)
         | MATCH (source)-[rel:`KNOWS`]->(target)
         | WHERE ((source.name = ${parameterNames("source.name_1")} OR target.name = ${parameterNames("target.name_1")} OR source.name = ${parameterNames("source.name_2")})
         | AND (target.age = ${parameterNames("target.age_1")} OR target.age = ${parameterNames("target.age_2")})
         | AND rel.score = ${parameterNames("rel.score")})
         | RETURN rel, source AS source, target AS target
         |""".stripMargin.replaceAll("\n", ""), query)
  }

  @Test
  def testCompoundKeysForNodes() = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("labels", "Location")
    options.put("node.keys", "LocationName:name,LocationType:type,FeatureID:featureId")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryWriteStrategy(SaveMode.Overwrite)).createQuery()

    assertEquals(
      """UNWIND $events AS event
        |MERGE (node:Location {name: event.keys.name, type: event.keys.type, featureId: event.keys.featureId})
        |SET node += event.properties
        |""".stripMargin, query)
  }

  @Test
  def testCompoundKeysForRelationship() = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "BOUGHT")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.source.node.keys", "FirstName:name,LastName:lastName")
    options.put("relationship.target.labels", "Product")
    options.put("relationship.target.node.keys", "ProductPrice:price,ProductId:id")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryWriteStrategy(SaveMode.Overwrite)).createQuery()

    assertEquals(
      """UNWIND $events AS event
        |MATCH (source:Person {name: event.source.keys.name, lastName: event.source.keys.lastName})
        |MATCH (target:Product {price: event.target.keys.price, id: event.target.keys.id})
        |MERGE (source)-[rel:BOUGHT]->(target)
        |SET rel += event.rel.properties
        |""".stripMargin, query.stripMargin)
  }

  @Test
  def testCompoundKeysForRelationshipMergeMatch() = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "BOUGHT")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.source.node.keys", "FirstName:name,LastName:lastName")
    options.put("relationship.source.save.mode", "Overwrite")
    options.put("relationship.target.labels", "Product")
    options.put("relationship.target.node.keys", "ProductPrice:price,ProductId:id")
    options.put("relationship.target.save.mode", "match")

    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryWriteStrategy(SaveMode.Overwrite)).createQuery()

    assertEquals(
      """UNWIND $events AS event
        |MERGE (source:Person {name: event.source.keys.name, lastName: event.source.keys.lastName}) SET source += event.source.properties
        |WITH source, event
        |MATCH (target:Product {price: event.target.keys.price, id: event.target.keys.id})
        |MERGE (source)-[rel:BOUGHT]->(target)
        |SET rel += event.rel.properties
        |""".stripMargin, query.stripMargin)
  }

  @Test
  def testShouldDoSumAggregationOnLabels(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val ageField = new DummyNamedReference("age")
    var query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionPagination.EMPTY, Seq("name",
        "SUM(DISTINCT age)",
        "SUM(age)"),
        Array(
          new Sum(ageField, false),
          new Sum(ageField, true)
        ))
    ).createQuery()

    assertEquals("MATCH (n:`Person`) RETURN n.name AS name, sum(DISTINCT n.age) AS `SUM(DISTINCT age)`, sum(n.age) AS `SUM(age)`", query)

    val nameField = new DummyNamedReference("name")
    query = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionPagination.EMPTY, Seq("name",
        "COUNT(DISTINCT name)",
        "COUNT(name)"),
        Array(
          new Count(nameField, false),
          new Count(nameField, true)
        ))
    ).createQuery()

    assertEquals("MATCH (n:`Person`) RETURN n.name AS name, count(DISTINCT n.name) AS `COUNT(DISTINCT name)`, count(n.name) AS `COUNT(name)`", query)

    query = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionPagination.EMPTY, Seq("name",
        "MAX(age)",
        "MIN(age)"),
        Array(
          new Max(ageField),
          new Min(ageField)
        ))
    ).createQuery()

    assertEquals("MATCH (n:`Person`) RETURN n.name AS name, max(n.age) AS `MAX(age)`, min(n.age) AS `MIN(age)`", query)
  }

  @Test
  def testShouldDoSumAggregationOnRelationships(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "BOUGHT")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Product")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val targetPriceField = new DummyNamedReference("`target.price`")
    var query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array.empty,
      PartitionPagination.EMPTY,
      List("source.fullName",
        "SUM(DISTINCT `target.price`)",
        "SUM(`target.price`)"),
      Array(
        new Sum(targetPriceField, false),
        new Sum(targetPriceField, true)
      )
    )).createQuery()

    assertEquals(
      """MATCH (source:`Person`)
        |MATCH (target:`Product`)
        |MATCH (source)-[rel:`BOUGHT`]->(target)
        |RETURN source.fullName AS `source.fullName`, sum(DISTINCT target.price) AS `SUM(DISTINCT ``target.price``)`, sum(target.price) AS `SUM(``target.price``)`"""
        .stripMargin
        .replaceAll("\n", " "), query)

    val targetIdField = new DummyNamedReference("`target.id`")
    query = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array.empty,
      PartitionPagination.EMPTY,
      List("source.fullName",
        "COUNT(DISTINCT `target.id`)",
        "COUNT(`target.id`)"),
      Array(
        new Count(targetIdField, false),
        new Count(targetIdField, true)
      )
    )).createQuery()

    assertEquals(
      """MATCH (source:`Person`) MATCH (target:`Product`)
        |MATCH (source)-[rel:`BOUGHT`]->(target)
        |RETURN source.fullName AS `source.fullName`, count(DISTINCT target.id) AS `COUNT(DISTINCT ``target.id``)`, count(target.id) AS `COUNT(``target.id``)`"""
        .stripMargin
        .replaceAll("\n", " "), query)

    query = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array.empty,
      PartitionPagination.EMPTY,
      List("source.fullName",
        "MAX(`target.price`)",
        "MIN(`target.price`)"),
      Array(
        new Max(targetPriceField),
        new Min(targetPriceField)
      )
    )).createQuery()

    assertEquals(
      """MATCH (source:`Person`)
        |MATCH (target:`Product`)
        |MATCH (source)-[rel:`BOUGHT`]->(target)
        |RETURN source.fullName AS `source.fullName`, max(target.price) AS `MAX(``target.price``)`, min(target.price) AS `MIN(``target.price``)`"""
        .stripMargin
        .replaceAll("\n", " "), query)
  }

  @Test
  def testTopNForLabels(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      partitionPagination = PartitionPagination(0, 0, TopN(42, Array(new SortOrder {
        override def expression(): Expression = new DummyNamedReference("name")

        override def direction(): SortDirection = SortDirection.ASCENDING

        override def nullOrdering(): NullOrdering = direction().defaultNullOrdering()
      }))))).createQuery()

    assertEquals("MATCH (n:`Person`) RETURN n ORDER BY n.name ASC LIMIT 42", query)
  }

  @Test
  def testTopNForLabelsWithRequiredColumn(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      requiredColumns = Array("name"),
      partitionPagination = PartitionPagination(0, 0, TopN(42, Array(new SortOrder {
        override def expression(): Expression = new DummyNamedReference("name")

        override def direction(): SortDirection = SortDirection.ASCENDING

        override def nullOrdering(): NullOrdering = direction().defaultNullOrdering()
      }))))).createQuery()

    assertEquals("MATCH (n:`Person`) RETURN n.name AS name ORDER BY n.name ASC LIMIT 42", query)
  }

  @Test
  def testTopNForRelationships(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array.empty[Filter],
      PartitionPagination(0, 0, TopN(24, Array(new SortOrder {
        override def expression(): Expression = new DummyNamedReference("rel.since")

        override def direction(): SortDirection = SortDirection.DESCENDING

        override def nullOrdering(): NullOrdering = direction().defaultNullOrdering()
      }))),
    )).createQuery()

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`) " +
      "MATCH (source)-[rel:`KNOWS`]->(target) RETURN rel, source AS source, target AS target " +
      "ORDER BY rel.since DESC LIMIT 24", query)
  }

  @Test
  def testTopNForRelationshipWithOneRequiredColumn(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array.empty[Filter],
      PartitionPagination(0, 0, TopN(24, Array(new SortOrder {
        override def expression(): Expression = new DummyNamedReference("rel.since")

        override def direction(): SortDirection = SortDirection.DESCENDING

        override def nullOrdering(): NullOrdering = direction().defaultNullOrdering()
      }))),
      Array("source.name")
    )).createQuery()

    assertEquals(
      """MATCH (source:`Person`)
        |MATCH (target:`Person`)
        |MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`
        |ORDER BY rel.since DESC LIMIT 24"""
        .stripMargin
        .replaceAll("\n", " "), query)
  }


  @Test
  def testTopNForCustomQueryIgnoresAggregation(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.QUERY.toString.toLowerCase, "MATCH (p:Person) RETURN p")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array.empty[Filter],
      PartitionPagination(0, 0, TopN(24, Array(new SortOrder {
        override def expression(): Expression = new DummyNamedReference("name")

        override def direction(): SortDirection = SortDirection.DESCENDING

        override def nullOrdering(): NullOrdering = direction().defaultNullOrdering()
      }))),
    )).createQuery()

    assertEquals("""WITH $scriptResult AS scriptResult
                   |MATCH (p:Person) RETURN p
                   |SKIP 0 LIMIT 24
                   |""".stripMargin, query)
  }
}
