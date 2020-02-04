package org.neo4j.spark;

import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 02.03.16
 */

public class Neo4jRDDTSE extends SparkConnectorScalaBaseTSE {

    public static final String QUERY1 = "MATCH (m:Movie {title:$title}) RETURN m.released as released";
    public static final String QUERY = "MATCH (m:Movie {title:$title}) RETURN m.released as released, m.tagline as tagline";
    public static final Map<String, Object> PARAMS = Collections.<String, Object>singletonMap("title", "The Matrix");
    public static final String FIXTURE = "CREATE (:Movie {title:'The Matrix', released:1999, tagline:'Welcome to the Real World'})";

    private Neo4JavaSparkContext csc;

    @Before
    public void before() {
        session().writeTransaction(tx -> tx.run(FIXTURE));
        csc = Neo4JavaSparkContext.neo4jContext(this.sc());
    }

    @Test
    public void runMatrixQuery() {
        List<Map<String, Object>> found = csc.query(QUERY, PARAMS).collect();

        assertEquals(1, found.size());
        assertEquals(1999L, found.get(0).get("released"));
        assertEquals("Welcome to the Real World", found.get(0).get("tagline"));
    }

    @Test
    public void runMatrixQueryTuple() {

        List<Map<String, Object>> found = csc.query(QUERY, PARAMS).collect();

        assertEquals(1, found.size());
        Map<String, Object> row = found.get(0);

        assertEquals(1999L, row.get("released"));
        assertEquals("Welcome to the Real World", row.get("tagline"));
    }

    @Test
    public void runMatrixQueryRow() {
        List<Row> found = csc.queryRow(QUERY, PARAMS).collect();
        assertEquals(1, found.size());
        Row row = found.get(0);

        assertEquals(2, row.size());
        assertEquals(1999L, row.getLong(0));
        assertEquals("Welcome to the Real World", row.getString(1));
    }
    @Test
    public void runMatrixQueryRow1() {
        List<Row> found = csc.queryRow(QUERY1, PARAMS).collect();
        assertEquals(1, found.size());
        Row row = found.get(0);

        assertEquals(1, row.size());
        assertEquals(1999L, row.getLong(0));
    }
}
