package org.neo4j.spark.cypher

case class NameProp(name: String, property: String = null) {
  def this(tuple: (String, String)) = this(tuple._1, tuple._2)

  def asTuple = (name, property)
}