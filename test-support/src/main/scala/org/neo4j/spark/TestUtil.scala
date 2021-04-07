package org.neo4j.spark

import org.neo4j.driver.{Session, Transaction}
import org.slf4j.Logger

import java.util.Properties

object TestUtil {

  private val properties = new Properties()
  properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("neo4j-spark-connector.properties"))

  def isCI(): Boolean = Option(System.getenv("CI")).getOrElse("false").toBoolean

  def neo4jVersion(): String = properties.getProperty("neo4j.version")

  def experimental(): Boolean = properties.getProperty("neo4j.experimental", "false").toBoolean

  def closeSafety(autoCloseable: AutoCloseable, logger: Logger = null): Unit = {
    try {
      autoCloseable match {
        case s: Session => if (s.isOpen) s.close()
        case t: Transaction => if (t.isOpen) t.close()
        case null => Unit
        case _ => autoCloseable.close()
      }
    } catch {
      case t: Throwable => if (logger != null) {
        t.printStackTrace()
        logger.warn(s"Cannot close ${autoCloseable.getClass.getSimpleName} because of the following exception:", t)
      }
    }
  }

}
