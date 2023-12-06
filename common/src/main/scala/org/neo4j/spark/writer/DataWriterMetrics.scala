package org.neo4j.spark.writer

import org.apache.spark.sql.connector.metric.{CustomMetric, CustomSumMetric, CustomTaskMetric}
import org.neo4j.driver.summary.SummaryCounters
import org.neo4j.spark.writer.DataWriterMetrics.{LABELS_ADDED, LABELS_REMOVED, NODES_CREATED, NODES_DELETED, PROPERTIES_SET, RECORDS_WRITTEN, RELATIONSHIPS_CREATED, RELATIONSHIPS_DELETED}

import java.util.concurrent.atomic.AtomicLong

case class DataWriterMetric(name: String, value: Long) extends CustomTaskMetric {
}

class DataWriterMetrics private(
                                 recordsProcessed: AtomicLong,
                                 nodesCreated: AtomicLong,
                                 nodesDeleted: AtomicLong,
                                 relationshipsCreated: AtomicLong,
                                 relationshipsDeleted: AtomicLong,
                                 propertiesSet: AtomicLong,
                                 labelsAdded: AtomicLong,
                                 labelsRemoved: AtomicLong) {

  def applyCounters(recordsWritten: Long, counters: SummaryCounters): Unit = {
    this.recordsProcessed.addAndGet(recordsWritten)
    this.nodesCreated.addAndGet(counters.nodesCreated())
    this.nodesDeleted.addAndGet(counters.nodesDeleted())
    this.relationshipsCreated.addAndGet(counters.relationshipsCreated())
    this.relationshipsDeleted.addAndGet(counters.relationshipsDeleted())
    this.propertiesSet.addAndGet(counters.propertiesSet())
    this.labelsAdded.addAndGet(counters.labelsAdded())
    this.labelsRemoved.addAndGet(counters.labelsRemoved())
  }

  def metricValues(): Array[CustomTaskMetric] = {
    List[CustomTaskMetric](
      DataWriterMetric(RECORDS_WRITTEN, recordsProcessed.longValue()),
      DataWriterMetric(NODES_CREATED, nodesCreated.longValue()),
      DataWriterMetric(NODES_DELETED, nodesDeleted.longValue()),
      DataWriterMetric(RELATIONSHIPS_CREATED, relationshipsCreated.longValue()),
      DataWriterMetric(RELATIONSHIPS_DELETED, relationshipsDeleted.longValue()),
      DataWriterMetric(PROPERTIES_SET, propertiesSet.longValue()),
      DataWriterMetric(LABELS_ADDED, labelsAdded.longValue()),
      DataWriterMetric(LABELS_REMOVED, labelsRemoved.longValue())
    ).toArray
  }

}

object DataWriterMetrics {
  final val RECORDS_WRITTEN = "neo4jMetrics.recordsWritten"
  final val RECORDS_WRITTEN_DESCRIPTION = "number of records written"
  final val NODES_CREATED = "neo4jMetrics.nodesCreated"
  final val NODES_CREATED_DESCRIPTION = "number of nodes created"
  final val NODES_DELETED = "neo4jMetrics.nodesDeleted"
  final val NODES_DELETED_DESCRIPTION = "number of nodes deleted"
  final val RELATIONSHIPS_CREATED = "neo4jMetrics.relationshipsCreated"
  final val RELATIONSHIPS_CREATED_DESCRIPTION = "number of relationships created"
  final val RELATIONSHIPS_DELETED = "neo4jMetrics.relationshipsDeleted"
  final val RELATIONSHIPS_DELETED_DESCRIPTION = "number of relationships deleted"
  final val PROPERTIES_SET = "neo4jMetrics.propertiesSet"
  final val PROPERTIES_SET_DESCRIPTION = "number of properties set"
  final val LABELS_ADDED = "neo4jMetrics.labelsAdded"
  final val LABELS_ADDED_DESCRIPTION = "number of labels added"
  final val LABELS_REMOVED = "neo4jMetrics.labelsRemoved"
  final val LABELS_REMOVED_DESCRIPTION = "number of labels removed"

  def apply(): DataWriterMetrics = {
    new DataWriterMetrics(
      new AtomicLong(0),
      new AtomicLong(0),
      new AtomicLong(0),
      new AtomicLong(0),
      new AtomicLong(0),
      new AtomicLong(0),
      new AtomicLong(0),
      new AtomicLong(0)
    )
  }

  def metricDeclarations(): Array[CustomMetric] = {
    List[CustomMetric](
      new RecordsWrittenMetric,
      new NodesCreatedMetric,
      new NodesDeletedMetric,
      new RelationshipsCreatedMetric,
      new RelationshipsDeletedMetric,
      new PropertiesSetMetric,
      new LabelsAddedMetric,
      new LabelsRemovedMetric
    ).toArray
  }

}

class RecordsWrittenMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.RECORDS_WRITTEN

  override def description(): String = DataWriterMetrics.RECORDS_WRITTEN_DESCRIPTION
}

class NodesCreatedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.NODES_CREATED

  override def description(): String = DataWriterMetrics.NODES_CREATED_DESCRIPTION
}

class NodesDeletedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.NODES_DELETED

  override def description(): String = DataWriterMetrics.NODES_DELETED_DESCRIPTION
}

class RelationshipsCreatedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.RELATIONSHIPS_CREATED

  override def description(): String = DataWriterMetrics.RELATIONSHIPS_CREATED_DESCRIPTION
}

class RelationshipsDeletedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.RELATIONSHIPS_DELETED

  override def description(): String = DataWriterMetrics.RELATIONSHIPS_DELETED_DESCRIPTION
}

class PropertiesSetMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.PROPERTIES_SET

  override def description(): String = DataWriterMetrics.PROPERTIES_SET_DESCRIPTION
}

class LabelsAddedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.LABELS_ADDED

  override def description(): String = DataWriterMetrics.LABELS_ADDED_DESCRIPTION
}

class LabelsRemovedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.LABELS_REMOVED

  override def description(): String = DataWriterMetrics.LABELS_REMOVED_DESCRIPTION
}
