name := "Spark App"
version := "1.0"
scalaVersion := "{exact-scala-version}"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "{spark-version}"
libraryDependencies += "org.neo4j" %% "neo4j-connector-apache-spark" % "{exact-connector-version}_for_spark_3"