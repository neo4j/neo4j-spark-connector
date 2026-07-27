# Neo4j Spark Connector v6 relocation

Since version 6.0.0 and later, we now use a different maven coordinate for our Spark connector.
If you plan to migrate from our v5 family of connectors, please use the new coordinate.

Version 6.0.0 introduces compatibility support for Apache Spark 4.0 and 4.1.
Since Apache Spark itself dropped support for Scala version 2.12, we followed suite and also dropped 2.12 support.
Therefore you will only find our v6 connector with support for Scala 2.13 and we only relocate our v5 variant that use Scala 2.13.

This is the maven relocation that was applied:

```patch
-org.neo4j:neo4j-connector-apache-spark_2.13:6.0.0_for_spark_3
+org.neo4j.connectors:spark:6.0.0-s_2.13
```
