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
import org.junit.jupiter.api.extension.AfterAllCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ExtensionContext.Namespace
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolutionException
import org.junit.jupiter.api.extension.ParameterResolver
import org.neo4j.caniuse.Neo4j
import org.neo4j.caniuse.Neo4jDetector
import org.neo4j.driver.Driver
import org.neo4j.spark.testsupport.Neo4jExtensions.DriverExtensions
import org.neo4j.spark.testsupport.Neo4jExtensions.Neo4jContainerExtensions
import org.neo4j.spark.testsupport.Neo4jTestResourcesExtension.log
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.neo4j.Neo4jContainer

import java.util.Objects

final case class Neo4jTestResources(
  container: Neo4jContainer,
  adminDriver: Driver,
  driver: Driver,
  spark: SparkSession,
  neo4j: Neo4j,
  user: String,
  password: String,
  database: String
) {

  def cleanup(): Unit = {
    var failure: Throwable = null

    container.synchronized {
      try {
        Option(spark).foreach(SparkSessions.release)
      } catch {
        case throwable: Throwable => failure = throwable
      }

      try {
        Option(driver).foreach(_.close())
      } catch {
        case throwable: Throwable =>
          if (failure != null) {
            failure.addSuppressed(throwable)
          } else {
            failure = throwable
          }
      }

      try {
        adminDriver.dropDatabase(database)
        adminDriver.dropUser(user)
      } catch {
        case throwable: Throwable =>
          if (failure != null) {
            failure.addSuppressed(throwable)
          } else {
            failure = throwable
          }
      } finally {
        try {
          adminDriver.close()
        } catch {
          case throwable: Throwable =>
            if (failure != null) {
              failure.addSuppressed(throwable)
            } else {
              failure = throwable
            }
        }
      }
    }

    if (failure != null) {
      throw failure
    }
  }
}

class Neo4jTestResourcesExtension
    extends BeforeEachCallback
    with AfterAllCallback
    with ParameterResolver {

  private val namespace = Namespace.create(classOf[Neo4jTestResourcesExtension])
  private val key = "resources"

  override def supportsParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Boolean =
    parameterContext.getParameter.getType match {
      case t if t == classOf[Neo4jTestResources] => true
      case t if t == classOf[Driver]             => true
      case t if t == classOf[SparkSession]       => true
      case t if t == classOf[Neo4j]              => true
      case _                                     => false
    }

  override def resolveParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): AnyRef =
    parameterContext.getParameter.getType match {
      case t if t == classOf[Neo4jTestResources] => resources(extensionContext)
      case t if t == classOf[Driver]             => resources(extensionContext).driver
      case t if t == classOf[SparkSession]       => resources(extensionContext).spark
      case t if t == classOf[Neo4j]              => resources(extensionContext).neo4j
      case other =>
        throw new ParameterResolutionException(
          s"Unsupported parameter type: ${other.getName}"
        )
    }

  override def beforeEach(context: ExtensionContext): Unit = {
    val testResources = resources(context)
    if (shouldResetBeforeEach(context)) {
      resetDatabase(testResources)
    }
  }

  override def afterAll(context: ExtensionContext): Unit = {
    Option(store(context).remove(key, classOf[Neo4jTestResources])).foreach(_.cleanup())
  }

  private def resources(context: ExtensionContext): Neo4jTestResources = {
    this.synchronized {
      val existing = store(context).get(key, classOf[Neo4jTestResources])
      if (existing != null) {
        existing
      } else {
        val created = createResources(context)
        store(context).put(key, created)
        created
      }
    }
  }

  private def createResources(context: ExtensionContext): Neo4jTestResources = {
    val testInstance = context.getRequiredTestInstance
    val container = readField(testInstance, "neo4jContainer").asInstanceOf[Neo4jContainer]

    val logConsumer = new Slf4jLogConsumer(log)
    if (context.getTestClass.isPresent) {
      logConsumer.withPrefix(context.getTestClass.get().getSimpleName)
    }
    container.withLogConsumer(logConsumer)

    container.startLazily()

    val suffix = Integer.toUnsignedString(Objects.hash(owningContext(context).getUniqueId, container.getBoltUrl), 36)
    val user = s"test-user-$suffix"
    val password = s"test-password-$suffix"
    val database = s"test-db-$suffix"

    val resources = container.synchronized {
      val adminDriver = container.driver()
      adminDriver.createOrReplaceUser(user, password, database)
      adminDriver.createOrReplaceDatabase(database)
      val driver = container.driver(user, password)
      val spark = container.spark(user, password)
      val neo4j = Neo4jDetector.INSTANCE.detect(driver)

      Neo4jTestResources(
        container,
        adminDriver,
        driver,
        spark,
        neo4j,
        user,
        password,
        database
      )
    }

    resources
  }

  private def resetDatabase(resources: Neo4jTestResources): Unit = {
    resources.container.synchronized {
      resources.adminDriver
        .executableQuery("MATCH (n) DETACH DELETE n")
        .withConfig(org.neo4j.driver.QueryConfig.builder().withDatabase(resources.database).build())
        .execute()
    }
  }

  private def shouldResetBeforeEach(context: ExtensionContext): Boolean =
    !List("org.neo4j.spark.GraphDataScienceIT", "org.neo4j.spark.WriteIT")
      .contains(context.getRequiredTestClass.getName)

  private def store(context: ExtensionContext) =
    owningContext(context).getStore(Namespace.create(namespace, context.getRequiredTestClass))

  private def owningContext(context: ExtensionContext): ExtensionContext =
    if (context.getTestMethod.isPresent) {
      context.getParent.orElse(context)
    } else {
      context
    }

  private def readField(target: AnyRef, fieldName: String): AnyRef = {
    var current: AnyRef = target
    while (current != null) {
      try {
        val field = current.getClass.getDeclaredField(fieldName)
        field.setAccessible(true)
        return field.get(current)
      } catch {
        case _: NoSuchFieldException =>
          current = outer(current).orNull
      }
    }

    throw new ParameterResolutionException(
      s"Could not find field '$fieldName' on test instance ${target.getClass.getName}"
    )
  }

  private def outer(target: AnyRef): Option[AnyRef] = {
    try {
      val field = target.getClass.getDeclaredField("$outer")
      field.setAccessible(true)
      Option(field.get(target))
    } catch {
      case _: NoSuchFieldException => None
    }
  }
}

object Neo4jTestResourcesExtension {
  private val log: Logger = LoggerFactory.getLogger(Neo4jTestResourcesExtension.getClass)
}
