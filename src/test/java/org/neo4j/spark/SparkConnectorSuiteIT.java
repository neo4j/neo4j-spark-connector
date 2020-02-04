package org.neo4j.spark;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({Neo4jDataFrameTSE.class, Neo4jGraphTSE.class, Neo4jRDDTSE.class})
public class SparkConnectorSuiteIT extends SparkConnectorScalaSuiteIT {}
