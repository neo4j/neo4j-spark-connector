package org.neo4j.spark.util

import org.hamcrest.CoreMatchers
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}
import org.neo4j.driver.AccessMode
import org.neo4j.spark.{SparkConnectorScalaSuiteIT, TestUtil}

import java.util.regex.Pattern

class ValidationsIT extends SparkConnectorScalaSuiteIT {

  val _expectedException: ExpectedException = ExpectedException.none

  @Rule
  def exceptionRule: ExpectedException = _expectedException

  @Test
  def testReadQueryShouldBeSyntacticallyInvalid(): Unit = {
    // then
    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage(CoreMatchers.containsString("Query not compiled for the following exception: ClientException: Invalid input "))
    val query = "MATCH (f{) RETURN f"
    _expectedException.expectMessage(CoreMatchers.containsString(query))

    // given
    val readOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    readOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    readOpts.put("query", query)

    // when
    Validations.validate(ValidateRead(new Neo4jOptions(readOpts), "1"))
  }

  @Test
  def testReadQueryShouldBeSemanticallyInvalid(): Unit = {
    // then
    val query = "MERGE (n:TestNode{id: 1}) RETURN n"
    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage(s"Invalid query `$query` because the accepted types are [READ_ONLY], but the actual type is READ_WRITE")

    // given
    val readOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    readOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    readOpts.put("query", query)

    // when
    Validations.validate(ValidateRead(new Neo4jOptions(readOpts), "1"))
  }

  @Test
  def testReadQueryCountBeSyntacticallyInvalid(): Unit = {
    // then
    val query = "MATCH (f{) RETURN f"
    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage(CoreMatchers.containsString("Query count not compiled for the following exception: ClientException: Invalid input "))
    _expectedException.expectMessage(CoreMatchers.containsString(s"EXPLAIN $query"))

    // given
    val readOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    readOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    readOpts.put("query", "MATCH (f) RETURN f")
    readOpts.put("query.count", query)

    // when
    Validations.validate(ValidateRead(new Neo4jOptions(readOpts), "1"))
  }

  @Test
  def testScriptQueryCountShouldContainAnInvalidQuery(): Unit = {
    // then
    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage(CoreMatchers.containsString("The following queries inside the `script` are not valid,"))
    _expectedException.expectMessage(CoreMatchers.containsString("Query not compiled for the following exception: ClientException: Invalid input "))
    _expectedException.expectMessage(CoreMatchers.containsString("EXPLAIN RETUR 2 AS two"))

    // given
    val readOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    readOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    readOpts.put("query", "MATCH (f) RETURN f")
    readOpts.put("script", "RETURN 1 AS one; RETUR 2 AS two; RETURN 3 AS three")

    // when
    Validations.validate(ValidateRead(new Neo4jOptions(readOpts), "1"))
  }

  @Test
  def testWriteQueryShouldBeSyntacticallyInvalid(): Unit = {
    // then
    val query = "MERGE (f{) RETURN f"
    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage(CoreMatchers.containsString("Query not compiled for the following exception: ClientException: Invalid input "))
    _expectedException.expectMessage(CoreMatchers.containsString(query))

    // given
    val writeOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    writeOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    writeOpts.put(Neo4jOptions.ACCESS_MODE, AccessMode.WRITE.toString)
    writeOpts.put("query", query)

    // when
    Validations.validate(ValidateWrite(new Neo4jOptions(writeOpts), "1", null))
  }

  @Test
  def testWriteQueryShouldBeSemanticallyInvalid(): Unit = {
    // then
    val query = "MATCH (n:TestNode{id: 1}) RETURN n"
    _expectedException.expect(classOf[IllegalArgumentException])
    _expectedException.expectMessage(s"Invalid query `$query` because the accepted types are [WRITE_ONLY, READ_WRITE], but the actual type is READ_ONLY")

    // given
    val writeOpts: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    writeOpts.put(Neo4jOptions.URL, SparkConnectorScalaSuiteIT.server.getBoltUrl)
    writeOpts.put(Neo4jOptions.ACCESS_MODE, AccessMode.WRITE.toString)
    writeOpts.put("query", query)

    // when
    Validations.validate(ValidateWrite(new Neo4jOptions(writeOpts), "1", null))
  }

}
