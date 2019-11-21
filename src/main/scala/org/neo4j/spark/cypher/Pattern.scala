package org.neo4j.spark.cypher

import org.neo4j.spark.cypher.CypherHelpers._

case class Pattern(source: NameProp, edges: Seq[NameProp], target: NameProp) {
  // fast count-queries for the partition sizes
  def countNode(node: NameProp) = s"MATCH (:${node.name.quote}) RETURN count(*) as total"

  def countRelsSource(rel: NameProp) = s"MATCH (:${source.name.quote})-[:${rel.name.quote}]->() RETURN count(*)"

  def countRelsTarget(rel: NameProp) = s"MATCH ()-[:${rel.name.quote}]->(:${target.name.quote}) RETURN count(*) AS total"

  def nodeQueries = List(nodeQuery(source), nodeQuery(target))

  def relQueries = edges.map(relQuery)

  def relQuery(rel: NameProp) = {
    val c: List[String] = List(countRelsSource(rel), countRelsTarget(rel))
    val q = s"MATCH (n:${source.name.quote})-[rel:${rel.name.quote}]->(m:${target.name.quote}) WITH n,rel,m SKIP $$_skip LIMIT $$_limit RETURN id(n) as src, id(m) as dst "
    if (rel.property != null) (q + s", rel.${rel.property.quote} as value", c)
    else (q, c)
  }

  def nodeQuery(node: NameProp) = {
    val c = countNode(node)
    val q: String = s"MATCH (n:${node.name.quote}) WITH n SKIP $$_skip LIMIT $$_limit RETURN id(n) AS id"
    if (node.property != null) (q + s", n.${node.property.quote} as value", c)
    else (q, c)
  }

  def this(source: (String, String), edges: Seq[(String, String)], target: (String, String)) =
    this(new NameProp(source), edges.map(new NameProp(_)), new NameProp(target))

  def this(source: String, edges: Seq[String], target: String) =
    this(NameProp(source), edges.map(NameProp(_)), NameProp(target))

  def edgeNames = edges.map(_.name)
}
