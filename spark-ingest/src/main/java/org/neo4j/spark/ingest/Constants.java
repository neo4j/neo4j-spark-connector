package org.neo4j.spark.ingest;

public final class Constants {

    private Constants() {}

    public static final String SOURCE_PARTITION = "_source_partition_column";
    public static final String TARGET_PARTITION = "_target_partition_column";
    public static final String RELATIONSHIP_PARTITION = "_relationship_partition_column";
    public static final String FORMAT = "org.neo4j.spark.DataSource";
}
