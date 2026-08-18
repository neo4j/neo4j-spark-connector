name := "Spark App"
version := "1.0"
scalaVersion := "{exact-scala-version}"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "{spark-version}"
libraryDependencies += "org.neo4j.connectors" %% "spark" % "{exact-connector-version}-s_{scala-version}"