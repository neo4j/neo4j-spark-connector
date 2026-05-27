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
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ParameterContext
import org.junit.jupiter.api.extension.ParameterResolver
import org.neo4j.driver._
import org.neo4j.spark.testsupport.Neo4jExtensions.Neo4jContainerExtensions
import org.testcontainers.neo4j.Neo4jContainer

import java.io.File
import java.lang.reflect.AnnotatedElement
import java.lang.reflect.Field
import java.nio.file.Files
import java.util.UUID

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

class DatabaseIsolationExtension extends BeforeEachCallback with AfterEachCallback with ParameterResolver {

  override def beforeEach(context: ExtensionContext): Unit = {
    setState(context)
  }

  override def afterEach(context: ExtensionContext): Unit = {
    val testState = unsetState(context)

    if (testState != null) {
      testState.cleanUp()
    }
  }

  override def supportsParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): Boolean = {
    val parameterType = parameterContext.getParameter.getType
    parameterType == classOf[Driver] ||
    parameterType == classOf[QueryConfig] ||
    parameterType == classOf[SparkSession]
  }

  override def resolveParameter(parameterContext: ParameterContext, extensionContext: ExtensionContext): AnyRef = {
    val testState = setState(extensionContext)
    val parameterType = parameterContext.getParameter.getType

    if (parameterType == classOf[Driver]) {
      testState.driver
    } else if (parameterType == classOf[QueryConfig]) {
      testState.queryConfig
    } else if (parameterType == classOf[SparkSession]) {
      testState.spark
    } else {
      throw new IllegalStateException(s"Unsupported parameter type: ${parameterType.getName}")
    }
  }

  private def setState(context: ExtensionContext): SparkTestState =
    context.getStore(DatabaseIsolationExtension.Namespace)
      .computeIfAbsent(
        DatabaseIsolationExtension.TestStateKey,
        (_: String) => SparkTestState.create(classState(context)),
        classOf[SparkTestState]
      )

  private def unsetState(context: ExtensionContext) = {
    context.getStore(DatabaseIsolationExtension.Namespace)
      .remove(DatabaseIsolationExtension.TestStateKey, classOf[SparkTestState])
  }

  private def sharedSparkState(context: ExtensionContext): SharedSparkState =
    context.getStore(ExtensionContext.StoreScope.LAUNCHER_SESSION, DatabaseIsolationExtension.Namespace)
      .computeIfAbsent(
        DatabaseIsolationExtension.SharedSparkKey,
        (_: String) => SharedSparkState(),
        classOf[SharedSparkState]
      )

  private def classState(context: ExtensionContext): ClassState = {
    val testInstance = locateTestInstance(context)
    val classContext = testClassContext(context, testInstance.getClass)

    classContext.getStore(DatabaseIsolationExtension.Namespace)
      .computeIfAbsent(
        DatabaseIsolationExtension.ClassStateKey,
        (_: String) => ClassState(containerOf(testInstance), sharedSparkState(context).spark),
        classOf[ClassState]
      )
  }

  private def locateTestInstance(context: ExtensionContext): AnyRef =
    context.getRequiredTestInstances.getAllInstances.asScala.headOption
      .getOrElse(throw new IllegalStateException("No test instance is available"))

  private def testClassContext(context: ExtensionContext, testClass: Class[_]): ExtensionContext = {
    Iterator.iterate(Option(context))(ctxOpt => ctxOpt.flatMap(ctx => Option(ctx.getParent.orElse(null))))
      .takeWhile(_.nonEmpty)
      .flatten
      .find(ctx => isTestClassElement(ctx.getElement.orElse(null), testClass))
      .getOrElse(throw new IllegalStateException(s"Cannot find class context for ${testClass.getName}"))
  }

  private def isTestClassElement(element: AnnotatedElement, testClass: Class[_]): Boolean = {
    element match {
      case clazz: Class[_] => clazz == testClass
      case _               => false
    }
  }

  private def containerOf(testInstance: AnyRef): Neo4jContainer = {
    val fields = Iterator.iterate[Class[_]](testInstance.getClass)(_.getSuperclass)
      .takeWhile(_ != null)
      .flatMap(_.getDeclaredFields)
      .filter(_.isAnnotationPresent(classOf[DatabaseIsolationContainer]))
      .toSeq

    fields match {
      case field +: Nil => readContainer(testInstance, field)
      case Nil => throw new IllegalStateException(
          s"${testInstance.getClass.getName} must annotate exactly one Neo4jContainer field with " +
            s"${classOf[DatabaseIsolationContainer].getName}"
        )
      case _ => throw new IllegalStateException(
          s"${testInstance.getClass.getName} must not annotate more than one Neo4jContainer field with " +
            s"${classOf[DatabaseIsolationContainer].getName}"
        )
    }
  }

  private def readContainer(testInstance: AnyRef, field: Field): Neo4jContainer = {
    field.setAccessible(true)
    field.get(testInstance) match {
      case container: Neo4jContainer => container
      case null => throw new IllegalStateException(
          s"Annotated Neo4jContainer field '${field.getName}' on ${testInstance.getClass.getName} is not initialized"
        )
      case value => throw new IllegalStateException(
          s"Annotated field '${field.getName}' on ${testInstance.getClass.getName} must be a Neo4jContainer, " +
            s"but was ${value.getClass.getName}"
        )
    }
  }
}

private object DatabaseIsolationExtension {
  val Namespace: ExtensionContext.Namespace = ExtensionContext.Namespace.create(classOf[DatabaseIsolationExtension])
  val ClassStateKey = "class-state"
  val SharedSparkKey = "shared-spark-state"
  val TestStateKey = "test-state"
}

private case class SharedSparkState()
    extends AutoCloseable {

  private val tmpDir: File = Files.createTempDirectory("spark-warehouse").toFile

  val spark: SparkSession = SparkSession.builder()
    .config(new SparkConf()
      .setAppName("neoTest")
      .setMaster("local[*]")
      .set("spark.driver.host", "127.0.0.1")
      .set("spark.sql.warehouse.dir", tmpDir.getAbsolutePath))
    .getOrCreate()

  override def close(): Unit = {
    TestUtil.closeSafely(spark)
  }
}

private case class ClassState(
  neo4jContainer: Neo4jContainer,
  rootSpark: SparkSession
) extends AutoCloseable {

  if (!neo4jContainer.isRunning) {
    neo4jContainer.start()
  }

  val driver: Driver = neo4jContainer.driver()

  override def close(): Unit = {
    driver.close()
    neo4jContainer.close()
  }
}

private object SparkTestState {

  def create(classState: ClassState): SparkTestState = {
    val databaseName = s"test${UUID.randomUUID().toString.replace("-", "")}"
    createDatabase(classState.driver, databaseName)

    SparkTestState(
      databaseName,
      QueryConfig.builder().withDatabase(databaseName).build(),
      classState.driver,
      newSparkSession(classState.rootSpark, classState.neo4jContainer, databaseName)
    )
  }

  private def createDatabase(driver: Driver, databaseName: String): Unit = {
    driver.executableQuery(s"CREATE DATABASE $$db WAIT 30 seconds")
      .withParameters(Map[String, AnyRef]("db" -> databaseName).asJava)
      .withConfig(QueryConfig.builder().withDatabase("system").build())
      .execute()
  }

  private def newSparkSession(
    rootSpark: SparkSession,
    neo4jContainer: Neo4jContainer,
    databaseName: String
  ): SparkSession = {
    val spark = rootSpark.newSession()
    spark.conf.set("neo4j.url", neo4jContainer.getBoltUrl)
    spark.conf.set("neo4j.authentication.basic.username", "neo4j")
    spark.conf.set("neo4j.authentication.basic.password", neo4jContainer.getAdminPassword)
    spark.conf.set("neo4j.database", databaseName)
    spark
  }
}

private case class SparkTestState(
  databaseName: String,
  queryConfig: QueryConfig,
  driver: Driver,
  spark: SparkSession
) {

  def cleanUp(): Unit = {
    spark.catalog.listTables().collect().foreach(table => spark.catalog.dropTempView(table.name))
    driver.executableQuery(s"DROP DATABASE $$db IF EXISTS WAIT 30 seconds")
      .withParameters(Map[String, AnyRef]("db" -> databaseName).asJava)
      .withConfig(QueryConfig.builder().withDatabase("system").build())
      .execute()
  }

}
