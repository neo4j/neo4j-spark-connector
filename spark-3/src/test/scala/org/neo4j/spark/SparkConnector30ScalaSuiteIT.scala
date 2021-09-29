package org.neo4j.spark

import org.junit.runner.RunWith
import org.junit.runners.Suite


@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(
  classOf[DataSourceReaderTSE],
  classOf[DataSourceReaderNeo4j4xTSE],
  classOf[DataSourceWriterNeo4j4xTSE],
  classOf[DataSourceReaderNeo4j35xTSE],
  classOf[DataSourceReaderNeo4j41xTSE],
  classOf[DataSourceWriterTSE],
  classOf[DefaultConfigTSE],
  classOf[DataSourceStreamingReaderTSE],
  classOf[DataSourceStreamingWriterTSE]
))
class SparkConnector30ScalaSuiteIT extends SparkConnectorScalaSuiteIT {}
