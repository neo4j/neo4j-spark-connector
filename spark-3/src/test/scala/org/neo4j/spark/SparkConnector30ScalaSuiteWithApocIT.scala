package org.neo4j.spark

import org.junit.runner.RunWith
import org.junit.runners.Suite


@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(
  classOf[DataSourceReaderWithApocTSE],
  classOf[DataSourceReaderNeo4jWithApocTSE],
  classOf[DataSourceReaderAggregationTSE]
))
class SparkConnector30ScalaSuiteWithApocIT extends SparkConnectorScalaSuiteWithApocIT {}
