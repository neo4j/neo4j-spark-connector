package org.neo4j.spark.dsl

import org.neo4j.spark.Neo4j

trait QueriesDsl {
  def cypher(cypher: String, params: Map[String, Any]) : Neo4j
  def params(params: Map[String, Any]) : Neo4j
  def param(key: String, value: Any) : Neo4j
  def nodes(cypher: String, params: Map[String, Any]) : Neo4j
  def rels(cypher : String, params : Map[String,Any]) : Neo4j
  def pattern(source: (String, String), edge: (String, String), target: (String, String)) : Neo4j
  def pattern(source: String, edges: Seq[String], target: String) : Neo4j
}
