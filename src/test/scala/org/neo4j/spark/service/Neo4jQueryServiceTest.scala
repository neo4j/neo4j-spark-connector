package org.neo4j.spark.service

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.{EqualNullSafe, EqualTo, Filter, GreaterThanOrEqual, LessThan, Not, Or, StringEndsWith, StringStartsWith}
import org.junit.Assert._
import org.junit.Test
import org.neo4j.spark.{Neo4jOptions, QueryType}

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
  def testNodeOneLabelWithOneSelectedColumn(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(
      neo4jOptions,
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionSkipLimit(0, -1, -1), Seq("name"))
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
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionSkipLimit(0, -1, -1), List("name", "bornDate"))
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
      new Neo4jQueryReadStrategy(Array.empty[Filter], PartitionSkipLimit(0, -1, -1), List("<id>"))
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

    assertEquals("MATCH (n:`Person`) WHERE n.name = 'John Doe' RETURN n", query)
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

    assertEquals("MATCH (n:`Person`) WHERE (((n.name IS NULL AND 'John Doe' IS NULL) OR n.name = 'John Doe') AND n.age = 36) RETURN n", query)
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

    assertEquals("MATCH (n:`Person`) WHERE (((n.name IS NULL AND NULL IS NULL) OR n.name = NULL) AND n.age = 36) RETURN n", query)
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

    assertEquals("MATCH (n:`Person`) WHERE (n.name STARTS WITH 'Person Name' AND n.name ENDS WITH 'Person Surname') RETURN n", query)
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
      PartitionSkipLimit(0, -1, -1),
      List("source.name")
    )).createQuery()

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`) " +
      "MATCH (source)-[rel:`KNOWS`]->(target) RETURN source.name AS `source.name`", query)
  }

  @Test
  def testRelationshipWithOneMoreColumnsSelected(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put("relationship", "KNOWS")
    options.put("relationship.nodes.map", "false")
    options.put("relationship.source.labels", "Person")
    options.put("relationship.target.labels", "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(
      Array[Filter](),
      PartitionSkipLimit(0, -1, -1),
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

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`) " +
      "MATCH (source)-[rel:`KNOWS`]->(target) WHERE source.name = 'John Doe' RETURN source, rel, target", query)
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

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`) " +
      "MATCH (source)-[rel:`KNOWS`]->(target) WHERE (source.name = 'John Doe' OR target.name = 'John Doe') RETURN source, rel, target", query)
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

    assertEquals(
      "MATCH (n:`Person`)" +
        " WHERE ((n.name = 'John Doe' OR n.name = 'John Scofield')" +
        " AND (n.age = 15 OR n.age >= 18)" +
        " AND (NOT (n.age = 22) OR NOT (n.age < 11)))" +
        " RETURN n", query)
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

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`:`Customer`) " +
      "MATCH (source)-[rel:`KNOWS`]->(target) " +
      "WHERE ((source.name = 'John Doe' OR target.name = 'John Doraemon' OR source.name = 'Jane Doe') " +
      "AND (target.age = 34 OR target.age = 18) " +
      "AND rel.score = 12) " +
      "RETURN source, rel, target", query)
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

    assertEquals("MATCH (source:`Person`) " +
      "MATCH (target:`Person`:`Customer`) " +
      "MATCH (source)-[rel:`KNOWS`]->(target) " +
      "WHERE ((source.name = 'John Doe' OR target.name = 'John Doraemon' OR source.name = 'Jane Doe') " +
      "AND (target.age = 34 OR target.age = 18) " +
      "AND rel.score = 12) " +
      "RETURN source, rel, target", query)
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
}
