package org.neo4j.spark.util

import org.apache.spark.sql.connector.expressions.NamedReference

class DummyNamedReference(private val fields: String*) extends NamedReference {
  override def fieldNames(): Array[String] = fields.toArray

  override def describe(): String = fields.mkString(", ")
  override def toString: String = describe()
}
