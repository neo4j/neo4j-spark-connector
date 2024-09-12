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
package org.neo4j.spark

import org.neo4j.driver.Session
import org.neo4j.driver.Transaction
import org.slf4j.Logger

import java.util.Properties

case class Version(major: Int, minor: Int, patch: Int) {

  override def toString: String = s"${major}.${minor}.${patch}"
}

object Version {
  implicit val ordering: Ordering[Version] = Ordering.by(v => (v.major, v.minor, v.patch))

  def parse(version: String): Version = {
    val fields = version.split("\\.")
      .map(_.toInt)
      .toList

    Version(fields.head, fields(1), fields(2))
  }

}

object Versions {

  val GDS_2_4: Version = Version(2, 4, 0)
  val GDS_2_5: Version = Version(2, 5, 0)
  val NEO4J_4_4: Version = Version(4, 4, 0)
  val NEO4J_5: Version = Version(5, 0, 0)
  val NEO4J_5_13: Version = Version(5, 13, 0)

}

object TestUtil {

  private val properties = new Properties()

  properties.load(
    Thread.currentThread().getContextClassLoader().getResourceAsStream("neo4j-spark-connector.properties")
  )

  def neo4jVersion(): Version = Version.parse(properties.getProperty("neo4j.version"))

  def gdsVersion(session: Session): Version = {
    Version.parse(session.run(
      "CALL gds.debug.sysInfo() YIELD key, value WHERE key = 'gdsVersion' RETURN value"
    ).single().get(0).asString())
  }

  def neo4jVersion(session: Session): Version = {
    Version.parse(session.run(
      "CALL dbms.components() YIELD name, versions WHERE name = 'Neo4j Kernel' RETURN versions[0]"
    ).single().get(0).asString())
  }

  def experimental(): Boolean = properties.getProperty("neo4j.experimental", "false").toBoolean

  def closeSafely(autoCloseable: AutoCloseable, logger: Logger = null): Unit = {
    try {
      autoCloseable match {
        case s: Session     => if (s.isOpen) s.close()
        case t: Transaction => if (t.isOpen) t.close()
        case null           => ()
        case _              => autoCloseable.close()
      }
    } catch {
      case t: Throwable => if (logger != null) {
          t.printStackTrace()
          logger.warn(s"Cannot close ${autoCloseable.getClass.getSimpleName} because of the following exception:", t)
        }
    }
  }

}
