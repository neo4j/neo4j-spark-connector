package org.neo4j.spark

import org.junit.runner.RunWith
import org.junit.runners.Suite
import org.neo4j.spark.service.SchemaServiceWithApocTSE

@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(
  classOf[SchemaServiceWithApocTSE]
))
class CommonTestSuiteWithApocIT extends SparkConnectorScalaSuiteWithApocIT {}
