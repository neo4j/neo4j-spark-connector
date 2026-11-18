package org.neo4j.spark.ingest.planner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RoundRobinPartitionPlanner extends PartitionPlanner {

    public RoundRobinPartitionPlanner(
            SparkSession spark, Dataset<Row> source, Column[] startColumns, Column[] endColumns, int workers) {
        super(spark, source, startColumns, endColumns, workers);
    }

    @Override
    public List<List<Partition>> plan() {
        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be positive");
        }

        var waves = new ArrayList<List<Partition>>();
        var diagonal = new ArrayList<Partition>();
        var partitions = new ArrayList<Integer>();

        for (int i = 0; i < partitionCount; i++) {
            diagonal.add(new Partition(i, i));
            partitions.add(i);
        }
        waves.add(diagonal);

        // For odd counts, one bucket rests each round.
        if (partitionCount % 2 != 0) {
            partitions.add(null);
        }

        // This is a mutable view: rotating it keeps the first bucket fixed.
        var rotatingPartitions = partitions.subList(1, partitions.size());

        for (int round = 0; round < partitions.size() - 1; round++) {
            var forward = new ArrayList<Partition>();
            var reverse = new ArrayList<Partition>();

            for (int i = 0; i < partitions.size() / 2; i++) {
                var source = partitions.get(i);
                var target = partitions.get(partitions.size() - 1 - i);

                if (source == null || target == null) {
                    continue;
                }

                forward.add(new Partition(source, target));
                reverse.add(new Partition(target, source));
            }

            if (!forward.isEmpty()) {
                waves.add(forward);
                waves.add(reverse);
            }

            Collections.rotate(rotatingPartitions, 1);
        }

        return waves;
    }
}
