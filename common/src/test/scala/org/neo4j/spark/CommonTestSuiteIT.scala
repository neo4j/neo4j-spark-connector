package org.neo4j.spark

import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.neo4j.spark.service.{Neo4jQueryServiceIT, SchemaServiceTSE}

@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(
  classOf[SchemaServiceTSE]
))
class CommonTestSuiteIT extends SparkConnectorScalaSuiteIT {}
