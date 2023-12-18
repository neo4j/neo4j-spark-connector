package org.neo4j.spark

import org.junit.runner.RunWith
import org.junit.runners.Suite


@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(
  classOf[DataSourceReaderTSE],
  classOf[DataSourceReaderNeo4jTSE],
  classOf[DataSourceWriterNeo4jTSE],
  classOf[DataSourceWriterTSE],
  classOf[DataSourceSchemaWriterTSE],
  classOf[DefaultConfigTSE],
  classOf[DataSourceStreamingReaderTSE],
  classOf[DataSourceStreamingWriterTSE],
  classOf[DataSourceReaderAggregationTSE]
))
class SparkConnector30ScalaSuiteIT extends SparkConnectorScalaSuiteIT {}
