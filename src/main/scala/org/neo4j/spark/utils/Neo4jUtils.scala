package org.neo4j.spark.utils

import org.neo4j.driver.{Driver, Session}

object Neo4jUtils {

  def close(driver: Driver, session: Session): Unit = {
    try {
      if (session != null && session.isOpen) {
        session.close()
      }
    } finally {
      if (driver != null) {
        driver.close()
      }
    }
  }

}
