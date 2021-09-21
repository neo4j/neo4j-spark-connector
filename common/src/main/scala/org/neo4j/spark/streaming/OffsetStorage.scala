package org.neo4j.spark.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import org.neo4j.driver.{Session, Transaction, TransactionWork, Values}
import org.neo4j.spark.util.{DriverCache, Neo4jOptions, Neo4jUtil, StorageType}

import java.lang
import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator
import scala.collection.JavaConverters.mapAsJavaMapConverter

object OffsetStorage {
  def register(jobId: String,
               initialValue: java.lang.Long = null,
               options: Neo4jOptions): OffsetStorage[lang.Long, lang.Long] = {
    val accumulator = options.streamingOptions.storageType match {
      case StorageType.SPARK => new SparkAccumulator(initialValue)
      case StorageType.NEO4J => new Neo4jAccumulator(options, initialValue)
    }
    val sparkSession = SparkSession.getActiveSession
      .getOrElse(throw new RuntimeException(s"""
           |Cannot register OffsetAccumulator for $jobId,
           |there is no Spark Session active
           |""".stripMargin))
    sparkSession.sparkContext.register(accumulator, jobId)
    accumulator
  }
}

abstract class OffsetStorage[IN, OUT] extends AccumulatorV2[IN, OUT] with AutoCloseable {
  def store(value: IN)
  def flush(): Unit
}


// N.b. the Neo4jAccumulator has been created as Spark 2.4 doesn't support accumulators
// from DatasourceV2, so the only way to check the last timestamp read by an Executor
// from the Driver is to store the metadata inside Neo4j and use it as external storage
object Neo4jAccumulator {
  val LABEL = "__Neo4jSparkStreamingMetadata"
  val KEY = "jobId"
  val LAST_TIMESTAMP = "lastTimestamp"
}
class Neo4jAccumulator(private val neo4jOptions: Neo4jOptions,
                       private val initialValue: lang.Long = null)
  extends SparkAccumulator(initialValue) {
  private lazy val driverCache = new DriverCache(neo4jOptions.connection, this.name.get)

  override def copy(): AccumulatorV2[lang.Long, lang.Long] = {
    val copy = new Neo4jAccumulator(neo4jOptions, super.value)
    copy
  }

  override def merge(other: AccumulatorV2[lang.Long, lang.Long]): Unit = add(other.value)

  override def close(): Unit = {
    val value = super.value
    if (value == null) return Unit
    var session: Session = null
    try {
      session = driverCache.getOrCreate().session(neo4jOptions.session.toNeo4jSession())
      session.writeTransaction(new TransactionWork[Unit] {
        override def execute(tx: Transaction): Unit = {
          tx.run(
              s"""
                 |MERGE (n:${Neo4jAccumulator.LABEL}{${Neo4jAccumulator.KEY}: ${'$'}jobId})
                 |ON CREATE SET n.${Neo4jAccumulator.LAST_TIMESTAMP} = ${'$'}value
                 |FOREACH (ignoreMe IN CASE WHEN n.${Neo4jAccumulator.LAST_TIMESTAMP} IS NOT NULL
                 | AND n.${Neo4jAccumulator.LAST_TIMESTAMP} < ${'$'}value THEN [1] ELSE []
                 | END | SET n.${Neo4jAccumulator.LAST_TIMESTAMP} = ${'$'}value
                 |)
                 |""".stripMargin,
              Map[String, AnyRef]("jobId" -> name.get, "value" -> value).asJava)
            .consume()
        }
      })
    } catch {
      case _: Throwable => //
    } finally {
      Neo4jUtil.closeSafety(session)
    }
  }

  override def flush(): Unit = {
    var session: Session = null
    try {
      session = driverCache.getOrCreate().session(neo4jOptions.session.toNeo4jSession())
      session.writeTransaction(new TransactionWork[Unit] {
        override def execute(tx: Transaction): Unit = {
          tx.run(
            s"""
              |MERGE (n:${Neo4jAccumulator.LABEL}{${Neo4jAccumulator.KEY}: ${'$'}jobId})
              |DELETE n
              |""".stripMargin,
            Map[String, AnyRef]("jobId" -> name.get).asJava)
            .consume()
        }
      })
    } catch {
      case _: Throwable => //
    } finally {
      Neo4jUtil.closeSafety(session)
      driverCache.close()
    }
  }

  override def value(): lang.Long = {
    var session: Session = null
    try {
      session = driverCache.getOrCreate().session(neo4jOptions.session.toNeo4jSession())
      session.readTransaction(new TransactionWork[lang.Long] {
        override def execute(tx: Transaction): lang.Long = {
          val currentValue = tx.run(
            s"""
              |MATCH (n:${Neo4jAccumulator.LABEL}{${Neo4jAccumulator.KEY}: ${'$'}jobId})
              |RETURN n.${Neo4jAccumulator.LAST_TIMESTAMP}
              |""".stripMargin,
            Map[String, AnyRef]("jobId" -> name.get).asJava)
            .single()
            .get(0)
          if (currentValue == Values.NULL) {
            Neo4jAccumulator.super.value
          } else {
            currentValue.asLong()
          }
        }
      })
    } catch {
      case _: Throwable => Neo4jAccumulator.super.value
    } finally {
      Neo4jUtil.closeSafety(session)
    }
  }
}

class SparkAccumulator(private val initialValue: lang.Long = null)
  extends OffsetStorage[lang.Long, lang.Long] {

  private val offset = new AtomicReference[lang.Long](initialValue)

  override def isZero: Boolean = offset.get() == null

  override def copy(): AccumulatorV2[lang.Long, lang.Long] = {
    val copy = new SparkAccumulator(offset.get())
    copy
  }

  override def reset(): Unit =  offset.set(null)

  override def add(newVal: lang.Long): Unit = offset.updateAndGet(new UnaryOperator[lang.Long] {
    override def apply(currVal: lang.Long): lang.Long = if (newVal != null && (currVal == null || newVal > currVal)) {
      newVal
    } else {
      currVal
    }
  })

  override def merge(other: AccumulatorV2[lang.Long, lang.Long]): Unit = add(other.value)

  override def value: lang.Long = offset.get()

  override def store(value: lang.Long): Unit = add(value)

  override def close(): Unit = Unit

  def flush(): Unit = Unit

}
