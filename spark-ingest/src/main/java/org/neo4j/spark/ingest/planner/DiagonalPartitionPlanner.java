package org.neo4j.spark.ingest.planner;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DiagonalPartitionPlanner extends PartitionPlanner {

    public DiagonalPartitionPlanner(
            SparkSession spark, Dataset<Row> source, Column[] startColumns, Column[] endColumns, int workers) {
        super(spark, source, startColumns, endColumns, workers);
    }

    @Override
    public List<List<Partition>> plan() {
        var p = IntStream.range(0, partitionCount)
                .mapToObj(diagonal -> IntStream.range(0, partitionCount)
                        .mapToObj(target -> new Partition((diagonal + target) % partitionCount, target))
                        .toList())
                .toList();

        for (var wave : p) {
            for (var partition : wave) {
                System.out.print(partition);
            }
            System.out.println();
        }

        return p;
    }
}
