package org.neo4j.spark

import org.junit.runner.RunWith
import org.junit.runners.Suite


@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(
  classOf[DataSourceReaderTSE],
  classOf[DataSourceReaderNeo4j4xTSE],
  classOf[DataSourceWriterTSE],
  classOf[DataSourceWriterNeo4j4xTSE],
  classOf[DataSourceReaderNeo4j35xTSE],
  classOf[DataSourceReaderNeo4j41xTSE],
  classOf[DefaultConfigTSE],
  classOf[DataSourceStreamingWriterTSE],
  classOf[DataSourceStreamingReaderTSE]
))
class SparkConnector24ScalaSuiteIT extends SparkConnectorScalaSuiteIT {}
