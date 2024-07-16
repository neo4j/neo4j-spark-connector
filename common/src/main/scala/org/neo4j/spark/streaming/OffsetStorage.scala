/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.spark.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2
import org.neo4j.driver.Session
import org.neo4j.driver.Transaction
import org.neo4j.driver.TransactionWork
import org.neo4j.driver.Values
import org.neo4j.driver.exceptions.NoSuchRecordException
import org.neo4j.spark.util.DriverCache
import org.neo4j.spark.util.Neo4jOptions
import org.neo4j.spark.util.Neo4jUtil
import org.neo4j.spark.util.StorageType

import java.lang
import java.util.concurrent.atomic.AtomicReference
import java.util.function.UnaryOperator

import scala.collection.JavaConverters.mapAsJavaMapConverter

object OffsetStorage {

  def register(
    jobId: String,
    initialValue: java.lang.Long = null,
    options: Neo4jOptions
  ): OffsetStorage[lang.Long, lang.Long] = {
    val accumulator = options.streamingOptions.storageType match {
      case StorageType.SPARK => new SparkAccumulator(initialValue)
      case StorageType.NEO4J => new Neo4jAccumulator(options, jobId, initialValue)
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

trait OffsetStorage[IN, OUT] extends AccumulatorV2[IN, OUT]
    with AutoCloseable
    with Logging {}

// N.b. the Neo4jAccumulator has been created as Spark 2.4 doesn't support accumulators
// from DatasourceV2, so the only way to check the last timestamp read by an Executor
// from the Driver is to store the metadata inside Neo4j and use it as external storage
object Neo4jAccumulator {
  val LABEL = "__Neo4jSparkStreamingMetadata"
  val KEY = "jobId"
  val LAST_TIMESTAMP = "lastTimestamp"
  val GUARDED_BY_LAST_CHECK = "guardedByLastCheck"
}

class Neo4jAccumulator(
  private val neo4jOptions: Neo4jOptions,
  private val jobId: String,
  private val initialValue: lang.Long = null
) extends OffsetStorage[lang.Long, lang.Long] {

  private lazy val driverCache = new DriverCache(neo4jOptions.connection, jobId)
  add(initialValue)

  override def copy(): AccumulatorV2[lang.Long, lang.Long] = new Neo4jAccumulator(neo4jOptions, jobId)

  override def merge(other: AccumulatorV2[lang.Long, lang.Long]): Unit = add(other.value)

  override def add(value: lang.Long): Unit = synchronized {
    if (value != null) {
      var session: Session = null
      try {
        session = driverCache.getOrCreate().session(neo4jOptions.session.toNeo4jSession())
        val dbVal = session.writeTransaction(new TransactionWork[lang.Long] {
          override def execute(tx: Transaction): lang.Long = {
            tx.run(
              s"""
                 |MERGE (n:${Neo4jAccumulator.LABEL}{${Neo4jAccumulator.KEY}: ${'$'}jobId})
                 |ON CREATE SET n.${Neo4jAccumulator.LAST_TIMESTAMP} = ${'$'}value
                 |FOREACH (ignoreMe IN CASE WHEN n.${Neo4jAccumulator.LAST_TIMESTAMP} IS NOT NULL
                 | AND n.${Neo4jAccumulator.LAST_TIMESTAMP} < ${'$'}value THEN [1] ELSE []
                 | END | SET n.${Neo4jAccumulator.LAST_TIMESTAMP} = ${'$'}value
                 |)
                 |SET n.${Neo4jAccumulator.GUARDED_BY_LAST_CHECK} = timestamp()
                 |RETURN n.${Neo4jAccumulator.LAST_TIMESTAMP}
                 |""".stripMargin,
              Map[String, AnyRef]("jobId" -> jobId, "value" -> value).asJava
            )
              .single()
              .get(0)
              .asLong()
          }
        })
        if (dbVal == value) {
          logDebug(s"Updated metadata state with value $value")
        } else {
          logDebug(s"Failed to updated metadata state with, provided value is $value, in the metadata state is $dbVal")
        }
      } catch {
        case e: Throwable =>
          logDebug(s"Error while updating the metadata state with value $value because of the following exception:", e)
      } finally {
        Neo4jUtil.closeSafely(session)
      }
    }
  }

  override def close(): Unit = synchronized {
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
            Map[String, AnyRef]("jobId" -> jobId).asJava
          )
            .consume()
        }
      })
    } catch {
      case e: Throwable =>
        logDebug(s"Error while cleaning the metadata state because of the following exception:", e)
    } finally {
      Neo4jUtil.closeSafely(session)
      driverCache.close()
    }
  }

  override def value(): lang.Long = synchronized {
    var session: Session = null
    try {
      session = driverCache.getOrCreate().session(neo4jOptions.session.toNeo4jSession())
      // we force the writeTransaction in order to be sure to reading
      // from the LEADER and having so the real last value
      session.writeTransaction(new TransactionWork[lang.Long] {
        override def execute(tx: Transaction): lang.Long = {
          val currentValue = tx.run(
            s"""
               |MATCH (n:${Neo4jAccumulator.LABEL}{${Neo4jAccumulator.KEY}: ${'$'}jobId})
               |SET n.${Neo4jAccumulator.GUARDED_BY_LAST_CHECK} = timestamp()
               |RETURN n.${Neo4jAccumulator.LAST_TIMESTAMP}
               |""".stripMargin,
            Map[String, AnyRef]("jobId" -> jobId).asJava
          )
            .single()
            .get(0)
          logDebug(s"Retrieved value from metadata state is: ${currentValue.asObject()}")
          if (currentValue == Values.NULL) {
            null
          } else {
            currentValue.asLong()
          }
        }
      })
    } catch {
      case e: Throwable => {
        if (!e.isInstanceOf[NoSuchRecordException]) {
          logDebug("Error while reading the metadata state:", e)
        }
        null
      }
    } finally {
      Neo4jUtil.closeSafely(session)
    }
  }

  override def isZero: Boolean = true

  override def reset(): Unit = ()
}

class SparkAccumulator(private val initialValue: lang.Long = null)
    extends OffsetStorage[lang.Long, lang.Long] {

  private val offset = new AtomicReference[lang.Long](initialValue)

  override def isZero: Boolean = offset.get() == null

  override def copy(): AccumulatorV2[lang.Long, lang.Long] = new SparkAccumulator(offset.get())

  override def reset(): Unit = offset.set(null)

  override def add(newVal: lang.Long): Unit = offset.updateAndGet(new UnaryOperator[lang.Long] {

    override def apply(currVal: lang.Long): lang.Long = if (newVal != null && (currVal == null || newVal > currVal)) {
      newVal
    } else {
      currVal
    }
  })

  override def merge(other: AccumulatorV2[lang.Long, lang.Long]): Unit = add(other.value)

  override def value(): lang.Long = offset.get()

  override def close(): Unit = ()

}
