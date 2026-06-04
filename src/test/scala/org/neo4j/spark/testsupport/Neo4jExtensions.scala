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

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.QueryConfig
import org.testcontainers.neo4j.Neo4jContainer

import scala.jdk.CollectionConverters.MapHasAsJava

object Neo4jExtensions {

  implicit class Neo4jContainerExtensions(container: Neo4jContainer) {

    def driver(): Driver = {
      val auth = AuthTokens.basic("neo4j", container.getAdminPassword)
      GraphDatabase.driver(container.getBoltUrl, auth)
    }

    def spark(): SparkSession = {
      SparkSession.builder()
        .config(new SparkConf()
          .setAppName("neoTest")
          .setMaster("local[*]")
          .set("spark.driver.host", "127.0.0.1")
          .set("neo4j.url", container.getBoltUrl)
          .set("neo4j.authentication.basic.username", "neo4j")
          .set("neo4j.authentication.basic.password", container.getAdminPassword)
          .set("neo4j.__internal_strict__", "true"))
        .getOrCreate()
    }
  }

  implicit class DriverExtensions(driver: Driver) {

    def createOrReplaceDatabase(database: String): Unit = {
      driver.executableQuery("CREATE OR REPLACE DATABASE $db WAIT 30 seconds")
        .withParameters(Map[String, AnyRef]("db" -> database).asJava)
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
