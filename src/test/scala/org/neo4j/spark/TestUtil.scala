package org.neo4j.spark

import java.util.Properties

object TestUtil {

  private val properties = new Properties()
  properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("neo4j-spark-connector.properties"))

  def isTravis(): Boolean = Option(System.getenv("TRAVIS")).getOrElse("false").toBoolean

  def neo4jVersion(): String = properties.getProperty("neo4j.version")

  def experimental(): Boolean = properties.getProperty("neo4j.experimental", "false").toBoolean

}
