package org.neo4j.spark.util

import org.apache.spark.sql.connector.expressions.NamedReference

abstract class DummyNamedReference extends NamedReference {
  override def toString: String = describe()
}
