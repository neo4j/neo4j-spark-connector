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
package org.neo4j.spark.testsupport

import org.apache.spark.sql.SparkSession
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.QueryConfig
import org.neo4j.spark.cypher.CypherRenderer
import org.neo4j.spark.util.Neo4jOptions
import org.testcontainers.neo4j.Neo4jContainer

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Using

object Neo4jExtensions {

  implicit class Neo4jContainerExtensions(container: Neo4jContainer) {

    def startLazily(): Unit = {
      if (!container.isRunning) {
        container.start()
      }
    }

    def cypherRenderer(options: Neo4jOptions = defaultNeo4jSparkOptions): CypherRenderer = {
      Using(driver()) { driver =>
        val neo4j = Neo4jDetector.INSTANCE.detect(driver)
        return new CypherRenderer(neo4j, options)
      }.get
    }

    def driver(): Driver = {
      val auth = AuthTokens.basic("neo4j", container.getAdminPassword)
      GraphDatabase.driver(container.getBoltUrl, auth)
    }

    def driver(username: String, password: String): Driver = {
      GraphDatabase.driver(container.getBoltUrl, AuthTokens.basic(username, password))
    }

    def spark(user: String, password: String): SparkSession = {
      SparkSessions.forContainer(container, user, password)
    }

    def authenticatedOptions(): Map[String, String] = {
      Map(
        "url" -> container.getBoltUrl,
        "authentication.basic.username" -> "neo4j",
        "authentication.basic.password" -> container.getAdminPassword
      )
    }

    private def defaultNeo4jSparkOptions = {
      val options = Map[String, String](
        "url" -> container.getBoltUrl,
        "username" -> "neo4j",
        "password" -> container.getAdminPassword,
        "query" -> "RETURN 42"
      )
      new Neo4jOptions(options)
    }
  }

  implicit class DriverExtensions(driver: Driver) {

    def createOrReplaceUser(user: String, password: String, homeDb: String): Unit = {
      driver.executableQuery("""
                               |CREATE OR REPLACE USER $user
                               |SET PASSWORD $password CHANGE NOT REQUIRED
                               |SET HOME DATABASE $homeDb
      """.stripMargin)
        .withParameters(Map[String, AnyRef]("user" -> user, "password" -> password, "homeDb" -> homeDb).asJava)
        .withConfig(QueryConfig.builder().withDatabase("system").build())
        .execute()
      driver.executableQuery(
        """
          |GRANT ROLE admin TO $user
          |""".stripMargin
      )
        .withParameters(Map[String, AnyRef]("user" -> user).asJava)
        .withConfig(QueryConfig.builder()
          .withDatabase("system")
          .build())
        .execute()
    }

    def createOrReplaceDatabase(database: String): Unit = {
      driver.executableQuery("CREATE OR REPLACE DATABASE $db WAIT 30 seconds")
        .withParameters(Map[String, AnyRef]("db" -> database).asJava)
        .withConfig(QueryConfig.builder().withDatabase("system").build())
        .execute()
    }

    def dropDatabase(database: String): Unit = {
      driver.executableQuery("DROP DATABASE $db IF EXISTS WAIT 30 seconds")
        .withParameters(Map[String, AnyRef]("db" -> database).asJava)
        .withConfig(QueryConfig.builder().withDatabase("system").build())
        .execute()
    }

    def dropUser(user: String): Unit = {
      driver.executableQuery("DROP USER $user IF EXISTS")
        .withParameters(Map[String, AnyRef]("user" -> user).asJava)
        .withConfig(QueryConfig.builder().withDatabase("system").build())
        .execute()
    }

    def serverSupportsApoc(): Boolean = {
      driver.executableQuery(
        "SHOW PROCEDURES YIELD name RETURN any(x IN collect(name) WHERE x STARTS WITH 'apoc.') AS hasApoc"
      )
        .execute()
        .records()
        .get(0)
        .get("hasApoc")
        .asBoolean()
    }

    def serverSupportsGds(): Boolean = {
      driver.executableQuery(
        "SHOW PROCEDURES YIELD name RETURN any(x IN collect(name) WHERE x STARTS WITH 'gds.') AS hasGDS"
      )
        .execute()
        .records()
        .get(0)
        .get("hasGDS")
        .asBoolean()
    }
  }
}
