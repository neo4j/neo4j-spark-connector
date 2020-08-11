package org.neo4j.spark

import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThanOrEqual, LessThan, Not, Or}
import org.junit.Assert._
import org.junit.Test
import org.neo4j.spark.service.{Neo4jQueryReadStrategy, Neo4jQueryService}

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
  def testNodeFilterEqualTo(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, "bolt://localhost")
    options.put(QueryType.LABELS.toString.toLowerCase, "Person")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val filters: Array[Filter] = Array[Filter](
      EqualTo("name", "John Doe")
    )

    val query: String = new Neo4jQueryService(neo4jOptions, new Neo4jQueryReadStrategy(filters)).createQuery()

    assertEquals("MATCH (n:`Person`) WHERE n.name = coalesce('John Doe', '') RETURN n", query)
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
      "MATCH (source)-[rel:`KNOWS`]->(target) WHERE source.name = coalesce('John Doe', '') RETURN source, rel, target", query)
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
      "MATCH (source)-[rel:`KNOWS`]->(target) WHERE (source.name = coalesce('John Doe', '') OR target.name = coalesce('John Doe', '')) RETURN source, rel, target", query)
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
        " WHERE ((n.name = coalesce('John Doe', '') OR n.name = coalesce('John Scofield', ''))" +
        " AND (n.age = coalesce(15, '') OR n.age >= coalesce(18, 0))" +
        " AND (NOT (n.age = coalesce(22, '')) OR NOT (n.age < coalesce(11, 0))))" +
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
      "WHERE ((source.name = coalesce('John Doe', '') OR target.name = coalesce('John Doraemon', '') OR source.name = coalesce('Jane Doe', '')) " +
      "AND (target.age = coalesce(34, '') OR target.age = coalesce(18, '')) " +
      "AND rel.score = coalesce(12, '')) " +
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
      "WHERE ((source.name = coalesce('John Doe', '') OR target.name = coalesce('John Doraemon', '') OR source.name = coalesce('Jane Doe', '')) " +
      "AND (target.age = coalesce(34, '') OR target.age = coalesce(18, '')) " +
      "AND rel.score = coalesce(12, '')) " +
      "RETURN source, rel, target", query)
  }
}