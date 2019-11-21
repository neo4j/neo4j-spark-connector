package org.neo4j.spark.cypher

import javax.lang.model.SourceVersion

object CypherHelpers {

  implicit class StringHelper(str: String) {
    def quote = if (SourceVersion.isIdentifier(str)) str else s"`$str`"
  }

}
