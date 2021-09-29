package org.neo4j.spark

import org.junit.runner.RunWith
import org.junit.runners.Suite


@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(
  classOf[DataSourceReaderWithApocTSE],
  classOf[DataSourceReaderNeo4j4xWithApocTSE],
  classOf[DataSourceReaderNeo4j41xWithApocTSE]
))
class SparkConnector24ScalaSuiteWithApocIT extends SparkConnectorScalaSuiteWithApocIT{}