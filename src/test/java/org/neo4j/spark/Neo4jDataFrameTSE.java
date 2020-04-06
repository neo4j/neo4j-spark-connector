package org.neo4j.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 02.03.16
 */

public class Neo4jDataFrameTSE extends SparkConnectorScalaBaseTSE {

    public static final String QUERY = "MATCH (m:Movie {title:$title}) RETURN m.released as released, m.tagline as tagline";
    public static final Map<String, Object> PARAMS = Collections.singletonMap("title", "The Matrix");
    public static final String FIXTURE = "CREATE (:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})";

    private Neo4JavaSparkContext csc;

    @Before
    public void before() {
        super.before();
        SparkConnectorScalaSuiteIT.session().writeTransaction(tx -> tx.run(FIXTURE));
        csc = Neo4JavaSparkContext.neo4jContext(this.sc());
    }

    @Test
    public void runMatrixQueryDFSchema() {
        Dataset<Row> found = csc.queryDF(QUERY, PARAMS,"released", "integer","tagline", "string").persist(); // we persist the dataset in order to create just one connection to the DB
        assertEquals(1, found.count());
        StructType schema = found.schema();
        assertEquals("long", schema.apply("released").dataType().typeName());
        assertEquals("string", schema.apply("tagline").dataType().typeName());

        Row row = found.first();

        assertEquals(2, row.size());
        assertEquals(1999L, row.getLong(0));
        assertEquals("Welcome to the Real World", row.getString(1));
    }

    @Test
    public void runMatrixQueryDF() {
        Dataset<Row> found = csc.queryDF(QUERY, PARAMS).persist(); // we persist the dataset in order to create just one connection to the DB
        assertEquals(1, found.count());
        StructType schema = found.schema();
        assertEquals("long", schema.apply("released").dataType().typeName());
        assertEquals("string", schema.apply("tagline").dataType().typeName());

        Row row = found.first();

        assertEquals(2, row.size());
        assertEquals(1999L, row.getLong(0));
        assertEquals("Welcome to the Real World", row.getString(1));
    }

}
