Neo4j Connector for Apache Spark {{projectVersion}}

This archive contains release materials for the Neo4j Connector for Apache Spark.

This archive contains two JARs, both built for Scala 2.13:

- neo4j-spark-connector-{{projectVersion}}-s_2.13.jar
  Bundles all dependencies. Use this one with `spark-submit --jars` or by copying it
  into your Spark environment's jars directory.
- neo4j-spark-connector-{{projectVersion}}-s_2.13-thin.jar
  Contains only the connector, its dependencies are declared in the Maven POM
  (org.neo4j.connectors:spark, classifier `thin`). Use this one if you manage your own
  classpath and need to avoid clashes with libraries such as Netty.

Source Code & Release Notes: https://github.com/neo4j/neo4j-spark-connector/releases
Documentation: https://neo4j.com/docs/spark/current/

Need Support?   Have a Question?

If you are a Neo4j Enterprise customer, please have a look at the enterprise
knowledge base, or contact Neo4j Professional Support through:
https://support.neo4j.com/hc/en-us

Otherwise, please consider posting a question on the Neo4j Community site:
https://community.neo4j.com/