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

import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.spark.testsupport.Neo4jContainerExtension.log
import org.rnorth.ducttape.unreliables.Unreliables
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.Neo4jContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy
import org.testcontainers.containers.wait.strategy.WaitAllStrategy

import java.time.Duration
import java.util.concurrent.TimeUnit

import scala.annotation.nowarn
import scala.io.Source
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.runtime.Nothing$

class DatabasesWaitStrategy(private val auth: AuthToken) extends AbstractWaitStrategy {
  private var databases = Seq.empty[String]

  def forDatabases(dbs: Seq[String]): DatabasesWaitStrategy = {
    databases ++= dbs
    this
  }

  override def waitUntilReady(): Unit = {
    val boltUrl = s"bolt://${waitStrategyTarget.getHost}:${waitStrategyTarget.getMappedPort(7687)}"
    val driver = GraphDatabase.driver(boltUrl, auth)
    val systemSession = driver.session(SessionConfig.forDatabase("system"))
    val tx = systemSession.beginTransaction()
    try {
      databases.foreach { db => tx.run(s"CREATE DATABASE $db IF NOT EXISTS") }
      tx.commit()
    } finally {
      tx.close()
    }

    try {

      Unreliables.retryUntilSuccess(
        startupTimeout.getSeconds.toInt,
        TimeUnit.SECONDS,
        () => {
          getRateLimiter.doWhenReady(() => {
            if (databases.nonEmpty) {
              val tx = systemSession.beginTransaction()
              val databasesStatus =
                try {
                  tx.run("SHOW DATABASES").list().asScala.map(db => {
                    (db.get("name").asString(), db.get("currentStatus").asString())
                  }).toMap
                } finally {
                  tx.close()
                }

              val notOnline = databasesStatus.filter(it => {
                it._2 != "online"
              })

              if (databasesStatus.size < databases.size || notOnline.nonEmpty) {
                throw new RuntimeException(s"Cannot started because of the following databases: ${notOnline.keys}")
              }
            }
          })
          true
        }
      )
    } finally {
      systemSession.close()
      driver.close()
    }
  }
}

@nowarn("cat=deprecation")
class Neo4jContainerExtension
    extends Neo4jContainer[Neo4jContainerExtension](
      TestUtil.neo4jImage()
    ) {
  private var databases: Seq[String] = Seq.empty

  private var fixture: Set[(String, String)] = Set.empty

  private var logPrefix: Option[String] = Option.empty

  def withDatabases(dbs: Seq[String]): Neo4jContainerExtension = {
    databases ++= dbs
    this
  }

  def withFixture(database: String, path: String): Neo4jContainerExtension = {
    fixture ++= Set((database, path))
    this
  }

  def withLogPrefix(prefix: String): Neo4jContainerExtension = {
    logPrefix = Some(prefix)
    this
  }

  private def createAuth(): AuthToken =
    if (getAdminPassword.nonEmpty) AuthTokens.basic("neo4j", getAdminPassword) else AuthTokens.none()

  override def start(): Unit = {
    if (databases.nonEmpty) {
      val waitAllStrategy = waitStrategy.asInstanceOf[WaitAllStrategy]
      waitAllStrategy.withStrategy(
        new DatabasesWaitStrategy(createAuth()).forDatabases(databases).withStartupTimeout(Duration.ofMinutes(2))
      )
    }
    val logConsumer = new Slf4jLogConsumer(log)
    for (value <- logPrefix) {
      logConsumer.withPrefix(value)
    }
    withLogConsumer(logConsumer)
    addEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    super.start()

    if (fixture.nonEmpty) {
      val driver = GraphDatabase.driver(this.getBoltUrl, createAuth())
      try {
        fixture.foreach(t => {
          val session = driver.session(SessionConfig.forDatabase(t._1))
          try {
            val lines = Source.fromResource(t._2)
              .mkString("\n")
              .split(";")
            lines.foreach(line => session.run(line))
          } finally {
            TestUtil.closeSafely(session)
          }
        })
      } finally {
        TestUtil.closeSafely(driver)
      }
    }
  }
}

object Neo4jContainerExtension {
  private val log: Logger = LoggerFactory.getLogger(Neo4jContainerExtension.getClass)
}
