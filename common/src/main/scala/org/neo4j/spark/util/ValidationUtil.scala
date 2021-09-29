package org.neo4j.spark.util

object ValidationUtil {

  def isNotEmpty(str: String, message: String) = if (str.isEmpty) {
    throw new IllegalArgumentException(message)
  }

  def isNotBlank(str: String, message: String) = if (str.trim.isEmpty) {
    throw new IllegalArgumentException(message)
  }

  def isBlank(str: String, message: String) = if (!str.trim.isEmpty) {
    throw new IllegalArgumentException(message)
  }

  def isNotEmpty(seq: Seq[_], message: String) = if (seq.isEmpty) {
    throw new IllegalArgumentException(message)
  }

  def isNotEmpty(map: Map[_, _], message: String) = if (map.isEmpty) {
    throw new IllegalArgumentException(message)
  }

  def isTrue(boolean: Boolean, message: String) = if (!boolean) {
    throw new IllegalArgumentException(message)
  }

  def isFalse(boolean: Boolean, message: String) = if (boolean) {
    throw new IllegalArgumentException(message)
  }

  def isNotValid(message: String) = throw new IllegalArgumentException(message)
}
