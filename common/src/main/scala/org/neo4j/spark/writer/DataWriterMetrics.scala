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
  final val RECORDS_WRITTEN = "recordsWritten"
  final val NODES_CREATED = "nodesCreated"
  final val NODES_DELETED = "nodesDeleted"
  final val RELATIONSHIPS_CREATED = "relationshipsCreated"
  final val RELATIONSHIPS_DELETED = "relationshipsDeleted"
  final val PROPERTIES_SET = "propertiesSet"
  final val LABELS_ADDED = "labelsAdded"
  final val LABELS_REMOVED = "labelsRemoved"

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

  override def description(): String = "number of records written"
}

class NodesCreatedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.NODES_CREATED

  override def description(): String = "number of nodes created"
}

class NodesDeletedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.NODES_DELETED

  override def description(): String = "number of nodes deleted"
}

class RelationshipsCreatedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.RELATIONSHIPS_CREATED

  override def description(): String = "number of relationships created"
}

class RelationshipsDeletedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.RELATIONSHIPS_DELETED

  override def description(): String = "number of relationships deleted"
}

class PropertiesSetMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.PROPERTIES_SET

  override def description(): String = "number of properties set"
}

class LabelsAddedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.LABELS_ADDED

  override def description(): String = "number of labels added"
}

class LabelsRemovedMetric extends CustomSumMetric {
  override def name(): String = DataWriterMetrics.LABELS_REMOVED

  override def description(): String = "number of labels removed"
}
