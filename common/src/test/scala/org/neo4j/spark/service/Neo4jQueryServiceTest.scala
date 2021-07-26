package org.neo4j.spark.service

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources._
import org.junit.Assert._
import org.junit.Test
import org.neo4j.spark.util.Neo4jImplicits.CypherImplicits
import org.neo4j.spark.util.{Neo4jOptions, Neo4jUtil, QueryType}

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

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(partitionSkipLimit = PartitionSkipLimit(0, 0, 100))).createQuery()

    assertEquals("MATCH (n:`Person`:`Player`:`Midfield`) RETURN n ORDER BY id(n) SKIP 0 LIMIT 100", query)
  }

  @Test
  def testNodeLabelWithNoSelectedColumns(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionSkipLimit.EMPTY, Seq())
    ).createQuery()

    assertEquals("MATCH (n:`Person`) RETURN n", query)
  }

  @Test
  def testNodeOneLabelWithOneSelectedColumn(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionSkipLimit.EMPTY, Seq("name"))
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
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionSkipLimit.EMPTY, List("name", "bornDate"))
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
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionSkipLimit.EMPTY, List("<id>"))
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
      Array[Filter](),
      PartitionSkipLimit.EMPTY,
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
      Array[Filter](),
      PartitionSkipLimit.EMPTY,
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
      Array[Filter](),
      PartitionSkipLimit(0, 0, 100),
      List("source.name", "<source.id>")
    )).createQuery()

    assertEquals("""MATCH (source:`Person`)
                   |MATCH (target:`Person`)
                   |MATCH (source)-[rel:`KNOWS`]->(target)
                   |RETURN source.name AS `source.name`, id(source) AS `<source.id>`
                   |ORDER BY id(rel)
                   |SKIP 0
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
      Array[Filter](),
      PartitionSkipLimit.EMPTY,
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
         | WHERE ((n.name = ${parameterNames("name_1")} OR n.name = ${parameterNames("name_2")})
         | AND (n.age = ${parameterNames("age_1")} OR n.age >= ${parameterNames("age_2")})
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
}
