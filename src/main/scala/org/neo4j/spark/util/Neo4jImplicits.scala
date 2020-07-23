package org.neo4j.spark.util

import javax.lang.model.SourceVersion

object Neo4jImplicits {
  implicit class CypherImplicits(str: String) {
    def quote(): String = if (!SourceVersion.isIdentifier(str) && !str.trim.startsWith("`")  && !str.trim.endsWith("`")) s"`$str`" else str
  }
}
