package org.neo4j.spark;

import org.apache.spark.graphx.Graph;
import org.junit.Before;
import org.junit.Test;
import scala.collection.Seq;
import scala.collection.Seq$;

import static org.junit.Assert.assertEquals;

/**
 * @author mh
 * @since 02.03.16
 */

public class Neo4jGraphTSE extends SparkConnectorScalaBaseTSE {

    public static final String FIXTURE = "CREATE (:A)-[:REL]->(:B)";

    @Before
    public void before() {
        session().writeTransaction(tx -> tx.run(FIXTURE));
    }

    @Test public void runMatrixQuery() {
        Seq<String> empty = (Seq<String>) Seq$.MODULE$.empty();
        Graph graph = Neo4jGraph.loadGraph(this.sc(), "A", empty, "B");
        assertEquals(2,graph.vertices().count());
        assertEquals(1,graph.edges().count());
    }
}
