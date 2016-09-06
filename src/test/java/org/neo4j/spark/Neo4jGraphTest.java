package org.neo4j.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import scala.Function1;
import scala.Tuple2;
import scala.collection.Seq$;
import scala.reflect.ClassTag$;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 02.03.16
 */

public class Neo4jGraphTest {

    public static final String FIXTURE = "CREATE (:A)-[:REL]->(:B)";

    private static SparkConf conf;
    private static JavaSparkContext sc;
    private static Neo4JavaSparkContext csc;
    private static ServerControls server;

    @BeforeClass
    public static void setUp() throws Exception {
        server = TestServerBuilders.newInProcessBuilder()
                .withConfig("dbms.security.auth_enabled","false")
                .withFixture(FIXTURE)
                .newServer();
        conf = new SparkConf()
                .setAppName("neoTest")
                .setMaster("local[*]")
                .set("spark.driver.allowMultipleContexts","true")
                .set("spark.neo4j.bolt.url", server.boltURI().toString());
        sc = new JavaSparkContext(conf);
        csc = Neo4JavaSparkContext.neo4jContext(sc);
    }

    @AfterClass
    public static void tearDown() {
        server.close();
        sc.close();
    }

    @Test public void runMatrixQuery() {
        Graph graph = Neo4jGraph.loadGraph(sc.sc(), "A", Seq$.MODULE$.empty() , "B");
        assertEquals(2,graph.vertices().count());
        assertEquals(1,graph.edges().count());
    }
}
