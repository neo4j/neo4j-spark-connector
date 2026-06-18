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
package org.neo4j.spark.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.jetbrains.annotations.TestOnly
import org.neo4j.connectors.authn.AuthenticationToken
import org.neo4j.connectors.authn.AuthenticationTokenSupplierFactory
import org.neo4j.connectors.common.driver.reauth.ReAuthDriverFactory
import org.neo4j.driver.Config.TrustStrategy
import org.neo4j.driver._
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.driver.net.ServerAddress
import org.neo4j.spark.util.Neo4jImplicits.StringMapImplicits

import java.io.File
import java.net.URI
import java.time.Duration
import java.util
import java.util.Locale
import java.util.ServiceLoader
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.function.Supplier

import scala.collection.JavaConverters._
import scala.language.implicitConversions

class Neo4jOptions(private val options: java.util.Map[String, String]) extends Serializable with Logging {

  import Neo4jOptions._
  import QueryType._

  def asMap() = new util.HashMap[String, String](options)

  private def getRequiredParameter(parameter: String): String = {
    if (!options.containsKey(parameter) || options.get(parameter).isEmpty) {
      throw new IllegalArgumentException(s"Parameter '$parameter' is required")
    }

    options.get(parameter)
  }

  private def getParameter(parameter: String, defaultValue: String = ""): String =
    getParameterOption(parameter).getOrElse(defaultValue)

  private def getParameterOption(parameter: String): Option[String] =
    Some(options.get(parameter))
      .flatMap(Option(_)) // to turn null into None
      .map(_.trim)

  private def getAuthenticationParameters: Map[String, String] = {
    val authType = getParameter(AUTH_TYPE, DEFAULT_AUTH_TYPE)
    val authNamespace = s"$AUTH.$authType"
    val providedParameters = options.asScala
      .filterKeys(_.startsWith(authNamespace))
      .map(t => (t._1.substring(authNamespace.length + 1), t._2))
      .toMap
    DEFAULT_AUTH_PARAMETERS ++ providedParameters
  }

  val saveMode: String = getParameter(SAVE_MODE, DEFAULT_SAVE_MODE.toString)

  val pushdownFiltersEnabled: Boolean =
    getParameter(PUSHDOWN_FILTERS_ENABLED, DEFAULT_PUSHDOWN_FILTERS_ENABLED.toString).toBoolean

  val pushdownColumnsEnabled: Boolean =
    getParameter(PUSHDOWN_COLUMNS_ENABLED, DEFAULT_PUSHDOWN_COLUMNS_ENABLED.toString).toBoolean

  val pushdownAggregateEnabled: Boolean =
    getParameter(PUSHDOWN_AGGREGATE_ENABLED, DEFAULT_PUSHDOWN_AGGREGATE_ENABLED.toString).toBoolean

  val pushdownLimitEnabled: Boolean =
    getParameter(PUSHDOWN_LIMIT_ENABLED, DEFAULT_PUSHDOWN_LIMIT_ENABLED.toString).toBoolean

  val pushdownTopNEnabled: Boolean =
    getParameter(PUSHDOWN_TOPN_ENABLED, DEFAULT_PUSHDOWN_TOPN_ENABLED.toString).toBoolean

  val schemaMetadata: Neo4jSchemaMetadata = initSchemaMetadata

  private def initSchemaMetadata = {
    val deprecatedSchemaOptimization = OptimizationType
      .withCaseInsensitiveName(getParameter(SCHEMA_OPTIMIZATION_TYPE, DEFAULT_OPTIMIZATION_TYPE.toString).toUpperCase)
    if (deprecatedSchemaOptimization != OptimizationType.NONE) {
      logWarning(
        s"""
           |Option `$SCHEMA_OPTIMIZATION_TYPE` is deprecated and will be removed in future implementations,
           |please move to one of the following depending on your use case:
           |- `$SCHEMA_OPTIMIZATION_NODE_KEY`
           |- `$SCHEMA_OPTIMIZATION_RELATIONSHIP_KEY`
           |""".stripMargin
      )
    }

    val nodeConstr: ConstraintsOptimizationType.Value = ConstraintsOptimizationType
      .withCaseInsensitiveName(getParameter(
        SCHEMA_OPTIMIZATION_NODE_KEY,
        DEFAULT_SCHEMA_OPTIMIZATION_NODE_KEY.toString
      ).trim)
    val relConstr: ConstraintsOptimizationType.Value = ConstraintsOptimizationType
      .withCaseInsensitiveName(getParameter(
        SCHEMA_OPTIMIZATION_RELATIONSHIP_KEY,
        DEFAULT_SCHEMA_OPTIMIZATION_RELATIONSHIP_KEY.toString
      ).trim)
    val schemaConstraints = getParameter(SCHEMA_OPTIMIZATION, DEFAULT_SCHEMA_OPTIMIZATION.toString)
      .split(",")
      .map(_.trim)
      .map(SchemaConstraintsOptimizationType.withCaseInsensitiveName)
      .toSet
    Neo4jSchemaMetadata(
      getParameter(SCHEMA_FLATTEN_LIMIT, DEFAULT_SCHEMA_FLATTEN_LIMIT.toString).toInt,
      SchemaStrategy.withCaseInsensitiveName(getParameter(
        SCHEMA_STRATEGY,
        DEFAULT_SCHEMA_STRATEGY.toString
      ).toUpperCase),
      deprecatedSchemaOptimization,
      Neo4jSchemaOptimizations(nodeConstr, relConstr, schemaConstraints),
      getParameter(SCHEMA_MAP_GROUP_DUPLICATE_KEYS, DEFAULT_MAP_GROUP_DUPLICATE_KEYS.toString).toBoolean
    )
  }

  val indexAwait = getParameter(INDEX_AWAIT_TIMEOUT_SEC, DEFAULT_INDEX_AWAIT_TIMEOUT_SEC.toString).toInt

  val query: Neo4jQueryOptions = (
    getParameter(QUERY.toString.toLowerCase),
    getParameter(LABELS.toString.toLowerCase),
    getParameter(RELATIONSHIP.toString.toLowerCase()),
    getParameter(GDS.toString.toLowerCase())
  ) match {
    case (query, "", "", "") => Neo4jQueryOptions(QUERY, query)
    case ("", label, "", "") => {
      val parsed = if (label.trim.startsWith(":")) label.substring(1) else label
      Neo4jQueryOptions(LABELS, parsed)
    }
    case ("", "", relationship, "") => Neo4jQueryOptions(RELATIONSHIP, relationship)
    case ("", "", "", gds)          => Neo4jQueryOptions(GDS, gds)
    case _ => throw new IllegalArgumentException(
        s"You need to specify just one of these options: ${
            QueryType.values.toSeq.map(value => s"'${value.toString.toLowerCase()}'")
              .sorted.mkString(", ")
          }"
      )
  }

  val connection: Neo4jDriverOptions = Neo4jDriverOptions(
    getRequiredParameter(URL),
    getParameter(AUTH_TYPE, DEFAULT_AUTH_TYPE),
    getAuthenticationParameters,
    getParameter(ENCRYPTION_ENABLED, DEFAULT_ENCRYPTION_ENABLED.toString).toBoolean,
    Option(getParameter(ENCRYPTION_TRUST_STRATEGY, null)),
    getParameter(ENCRYPTION_CA_CERTIFICATE_PATH, DEFAULT_EMPTY),
    getParameter(CONNECTION_MAX_LIFETIME_MSECS, DEFAULT_CONNECTION_MAX_LIFETIME_MSECS.toString).toInt,
    getParameter(CONNECTION_ACQUISITION_TIMEOUT_MSECS, DEFAULT_TIMEOUT.toString).toInt,
    getParameter(
      CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS,
      DEFAULT_CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS.toString
    ).toInt,
    getParameter(CONNECTION_TIMEOUT_MSECS, DEFAULT_TIMEOUT.toString).toInt
  )

  val session: Neo4jSessionOptions = Neo4jSessionOptions(
    getParameter(DATABASE, DEFAULT_EMPTY),
    AccessMode.valueOf(getParameter(ACCESS_MODE, DEFAULT_ACCESS_MODE.toString).toUpperCase())
  )

  val nodeMetadata: Neo4jNodeMetadata = initNeo4jNodeMetadata()

  val legacyTypeConversionEnabled: Boolean = getParameter(
    TYPE_CONVERSION,
    DEFAULT_TYPE_CONVERSION
  ).toLowerCase(Locale.ROOT).equals("legacy")

  private def mapPropsString(strOpt: Option[String]): Option[Map[String, String]] = strOpt.map(str =>
    str.split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(s => {
        val keys = if (s.startsWith("`")) {
          val pattern = "`[^`]+`".r
          val groups = pattern findAllIn s
          groups
            .map(_.replaceAll("`", ""))
            .toArray
        } else {
          s.split(":")
        }
        if (keys.length == 2) {
          (keys(0), keys(1))
        } else {
          (keys(0), keys(0))
        }
      })
      .toMap
  )

  private def initNeo4jNodeMetadata(
    nodeKeysString: String = getParameter(NODE_KEYS, ""),
    labelsString: String = query.value,
    nodePropsString: String = "",
    skipNullKeys: Boolean = getParameter(NODE_KEYS_SKIP_NULLS, "false").toBoolean
  ): Neo4jNodeMetadata = {

    val nodeKeys = mapPropsString(Some(nodeKeysString)).getOrElse(Map.empty[String, String])
    val nodeProps = mapPropsString(Some(nodePropsString)).getOrElse(Map.empty[String, String])

    val labels = labelsString
      .split(":")
      .map(_.trim)
      .filter(_.nonEmpty)
    Neo4jNodeMetadata(labels, nodeKeys, nodeProps, skipNullKeys)
  }

  val transactionSettings: Neo4jTransactionSettings = initNeo4jTransactionSettings()

  val script: Array[String] = extractScript()

  private def extractScript(): Array[String] = {
    val fromScript = getParameter(SCRIPT)
      .split(";")
      .map(_.trim)
      .filterNot(_.isEmpty)

    val fromScriptOptions = options.asScala
      .filterKeys(_.startsWith(SCRIPT_PREFIX))
      .map { case (key, value) =>
        val index = key.substring(SCRIPT_PREFIX.length)
        if (!index.matches("\\d+")) {
          throw new IllegalArgumentException(
            s"Script option '$key' must have a suffix containing only digits."
          )
        }
        (index.toInt, value)
      }
      .toSeq
      .sortBy(_._1)
      .map(_._2.trim)
      .toArray

    if (fromScript.nonEmpty && fromScriptOptions.nonEmpty) {
      throw new IllegalArgumentException(
        "'script' and indexed script options ('script.1', 'script.2', etc.) cannot be used together."
      )
    }

    if (fromScript.nonEmpty) {
      if (fromScript.length > 1) {
        logWarning(
          "Using multiple statements in the 'script' option is deprecated. Use indexed script options ('script.1', 'script.2', etc.) instead."
        )
      }
      return fromScript
    }

    fromScriptOptions
  }

  private def initNeo4jTransactionSettings(): Neo4jTransactionSettings = {
    val retries = getParameter(TRANSACTION_RETRIES, DEFAULT_TRANSACTION_RETRIES.toString).toInt
    val failOnTransactionCodes = getParameter(TRANSACTION_CODES_FAIL, DEFAULT_EMPTY)
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .toSet
    val batchSize = getParameter(BATCH_SIZE, DEFAULT_BATCH_SIZE.toString).toInt
    val retryTimeout = getParameter(TRANSACTION_RETRY_TIMEOUT, DEFAULT_TRANSACTION_RETRY_TIMEOUT.toString).toInt
    Neo4jTransactionSettings(retries, failOnTransactionCodes, batchSize, retryTimeout)
  }

  val relationshipMetadata: Neo4jRelationshipMetadata = initNeo4jRelationshipMetadata()

  private def initNeo4jRelationshipMetadata(): Neo4jRelationshipMetadata = {
    val source = initNeo4jNodeMetadata(
      getParameter(RELATIONSHIP_SOURCE_NODE_KEYS, ""),
      getParameter(RELATIONSHIP_SOURCE_LABELS, ""),
      getParameter(RELATIONSHIP_SOURCE_NODE_PROPS, ""),
      getParameter(RELATIONSHIP_SOURCE_NODE_KEYS_SKIP_NULLS, "false").toBoolean
    )

    val target = initNeo4jNodeMetadata(
      getParameter(RELATIONSHIP_TARGET_NODE_KEYS, ""),
      getParameter(RELATIONSHIP_TARGET_LABELS, ""),
      getParameter(RELATIONSHIP_TARGET_NODE_PROPS, ""),
      getParameter(RELATIONSHIP_TARGET_NODE_KEYS_SKIP_NULLS, "false").toBoolean
    )

    val nodeMap = getParameter(RELATIONSHIP_NODES_MAP, DEFAULT_RELATIONSHIP_NODES_MAP.toString).toBoolean

    val relProps = mapPropsString(getParameterOption(RELATIONSHIP_PROPERTIES))

    val writeStrategy = RelationshipSaveStrategy.withCaseInsensitiveName(getParameter(
      RELATIONSHIP_SAVE_STRATEGY,
      DEFAULT_RELATIONSHIP_SAVE_STRATEGY.toString
    ).toUpperCase)
    val sourceSaveMode = NodeSaveMode.withCaseInsensitiveName(getParameter(
      RELATIONSHIP_SOURCE_SAVE_MODE,
      DEFAULT_RELATIONSHIP_SOURCE_SAVE_MODE.toString
    ))
    val targetSaveMode = NodeSaveMode.withCaseInsensitiveName(getParameter(
      RELATIONSHIP_TARGET_SAVE_MODE,
      DEFAULT_RELATIONSHIP_TARGET_SAVE_MODE.toString
    ))

    val relationshipKeys = mapPropsString(getParameterOption(RELATIONSHIP_KEYS)).getOrElse(Map.empty)

    Neo4jRelationshipMetadata(
      source,
      target,
      sourceSaveMode,
      targetSaveMode,
      relProps,
      query.value,
      nodeMap,
      writeStrategy,
      relationshipKeys,
      getParameter(RELATIONSHIP_KEYS_SKIP_NULLS, "false").toBoolean
    )
  }

  private def initNeo4jQueryMetadata(): Neo4jQueryMetadata = Neo4jQueryMetadata(
    query.value.trim,
    getParameter(QUERY_COUNT, "").trim
  )

  val queryMetadata: Neo4jQueryMetadata = initNeo4jQueryMetadata()

  private def initNeo4jGdsMetadata(): Neo4jGdsMetadata = Neo4jGdsMetadata(
    options.asScala
      .filterKeys(k => k.startsWith("gds."))
      .map(t => (t._1.substring("gds.".length), t._2))
      .toMap
      .toNestedJavaMap
  )

  val gdsMetadata: Neo4jGdsMetadata = initNeo4jGdsMetadata()

  val partitions: Int = getParameter(PARTITIONS, DEFAULT_PARTITIONS.toString).toInt

  val streamingOrderBy: String = getParameter(ORDER_BY, getParameter(STREAMING_PROPERTY_NAME))

  val apocConfig: Neo4jApocConfig = Neo4jApocConfig(options.asScala
    .filterKeys(_.startsWith("apoc."))
    .mapValues(Neo4jUtil.mapper.readValue(_, classOf[java.util.Map[String, AnyRef]]).asScala)
    .toMap)

  def getTableName: String = query.queryType match {
    case QueryType.LABELS => s"table_${nodeMetadata.labels.mkString("-")}"
    case QueryType.RELATIONSHIP => s"table_${relationshipMetadata.source.labels.mkString("-")}" +
        s"_${relationshipMetadata.relationshipType}" +
        s"_${relationshipMetadata.target.labels.mkString("-")}"
    case _ => s"table_query_${UUID.randomUUID()}"
  }

  val streamingOptions: Neo4jStreamingOptions = Neo4jStreamingOptions(
    getParameter(STREAMING_PROPERTY_NAME),
    StreamingFrom.withCaseInsensitiveName(getParameter(STREAMING_FROM, DEFAULT_STREAMING_FROM.toString)),
    getParameter(STREAMING_QUERY_OFFSET)
  )

  def toNeo4jTransactionConfig: TransactionConfig = {
    val timeout = getParameter(TRANSACTION_TIMEOUT_MSECS, DEFAULT_TRANSACTION_TIMEOUT)

    val builder = TransactionConfig.builder()
    if (timeout != null) {
      val duration = Duration.ofMillis(timeout.toInt)
      builder.withTimeout(duration)
    }

    builder.build()
  }

}

case class Neo4jStreamingOptions(
  propertyName: String,
  from: StreamingFrom.Value,
  queryOffset: String
)

case class Neo4jApocConfig(procedureConfigMap: Map[String, AnyRef])

case class Neo4jSchemaOptimizations(
  nodeConstraint: ConstraintsOptimizationType.Value,
  relConstraint: ConstraintsOptimizationType.Value,
  schemaConstraints: Set[SchemaConstraintsOptimizationType.Value]
)

case class Neo4jSchemaMetadata(
  flattenLimit: Int,
  strategy: SchemaStrategy.Value,
  optimizationType: OptimizationType.Value,
  optimization: Neo4jSchemaOptimizations,
  mapGroupDuplicateKeys: Boolean
)

case class Neo4jTransactionSettings(
  retries: Int,
  failOnTransactionCodes: Set[String],
  batchSize: Int,
  retryTimeout: Long
) {

  def shouldFailOn(exception: Throwable): Boolean = {
    exception match {
      case e: Neo4jException => failOnTransactionCodes.contains(e.code())
      case _                 => false
    }
  }

}

case class Neo4jNodeMetadata(
  labels: Seq[String],
  nodeKeys: Map[String, String],
  properties: Map[String, String],
  skipNullKeys: Boolean = false
) {
  def includesProperty(name: String): Boolean = nodeKeys.contains(name) || properties.contains(name)
}

case class Neo4jRelationshipMetadata(
  source: Neo4jNodeMetadata,
  target: Neo4jNodeMetadata,
  sourceSaveMode: NodeSaveMode.Value,
  targetSaveMode: NodeSaveMode.Value,
  properties: Option[Map[String, String]],
  relationshipType: String,
  nodeMap: Boolean,
  saveStrategy: RelationshipSaveStrategy.Value,
  relationshipKeys: Map[String, String],
  skipNullKeys: Boolean = false
)

case class Neo4jQueryMetadata(query: String, queryCount: String)

case class Neo4jGdsMetadata(parameters: util.Map[String, Any])

case class Neo4jQueryOptions(queryType: QueryType.Value, value: String)

case class Neo4jSessionOptions(database: String, accessMode: AccessMode = AccessMode.READ) {

  def toNeo4jSession(): SessionConfig = {
    val builder = SessionConfig.builder()
      .withDefaultAccessMode(accessMode)

    if (database != null && database != "") {
      builder.withDatabase(database)
    }

    builder.build()
  }
}

case class Neo4jDriverOptions(
  url: String,
  auth: String,
  authParameters: Map[String, String],
  encryption: Boolean,
  trustStrategy: Option[String],
  certificatePath: String,
  lifetime: Int,
  acquisitionTimeout: Int,
  livenessCheckTimeout: Int,
  connectionTimeout: Int
) extends Serializable {

  def createDriver(): Driver = {
    val (url, _) = connectionUrls
    ReAuthDriverFactory.driver(url, createAuthTokenSupplier, toDriverConfig)
  }

  private def toDriverConfig: Config = {
    val builder = Config.builder()
      .withUserAgent(s"neo4j-${Neo4jUtil.connectorEnv}-connector/${Neo4jUtil.connectorVersion}")
      .withLogging(Logging.slf4j())

    if (lifetime > -1) builder.withMaxConnectionLifetime(lifetime, TimeUnit.MILLISECONDS)
    if (acquisitionTimeout > -1) builder.withConnectionAcquisitionTimeout(acquisitionTimeout, TimeUnit.MILLISECONDS)
    if (livenessCheckTimeout > -1)
      builder.withConnectionLivenessCheckTimeout(livenessCheckTimeout, TimeUnit.MILLISECONDS)
    if (connectionTimeout > -1) builder.withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)

    val (primaryUrl, resolvers) = connectionUrls

    primaryUrl.getScheme match {
      case "neo4j+s" | "neo4j+ssc" | "bolt+s" | "bolt+ssc" => ()
      case _ => {
        if (!encryption) {
          builder.withoutEncryption()
        } else {
          builder.withEncryption()
        }
        trustStrategy
          .map(Config.TrustStrategy.Strategy.valueOf)
          .map {
            case TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES              => TrustStrategy.trustAllCertificates()
            case TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES => TrustStrategy.trustSystemCertificates()
            case TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES =>
              TrustStrategy.trustCustomCertificateSignedBy(new File(certificatePath))
          }.foreach(builder.withTrustStrategy)
      }
    }

    if (resolvers.nonEmpty) {
      builder.withResolver(_ => resolvers.asJava)
    }

    builder.build()
  }

  // public only for testing purposes
  @TestOnly
  def connectionUrls: (URI, Set[ServerAddress]) = {
    val urls = url.split(",").toList
    val resolved = urls
      .drop(1)
      .map(_.trim)
      .map(URI.create)
      .map(uri => ServerAddress.of(uri.getHost, if (uri.getPort > -1) uri.getPort else 7687))
      .toSet
    (URI.create(urls.head.trim), resolved)
  }

  private def createAuthTokenSupplier: Supplier[AuthenticationToken] = {
    if (auth == null || auth.isEmpty) {
      throw new IllegalArgumentException(s"Authentication type name is required")
    }
    val supplierFactories = ServiceLoader.load(
      classOf[AuthenticationTokenSupplierFactory],
      getClass.getClassLoader
    ).iterator()
      .asScala
      .toList

    val filteredSupplierFactories = supplierFactories.filter(s => s.getName != null && s.getName.equalsIgnoreCase(auth))

    if (filteredSupplierFactories.isEmpty) {
      throw new IllegalArgumentException(
        s"Authentication method '$auth' is not supported. Supported authentication methods are: ${supplierFactories.map(_.getName).mkString(", ")}"
      )
    }
    if (filteredSupplierFactories.size > 1) {
      throw new IllegalArgumentException(
        s"Multiple implementation for authentication type '$auth' are found"
      )
    }

    val username = authParameters.get("username")
    val password = authParameters.get("password")
    filteredSupplierFactories.head.create(username.orNull, password.orNull, authParameters.asJava)
  }

}

object Neo4jOptions {

  // connection options
  val URL = "url"

  // auth
  val AUTH = "authentication"
  val AUTH_TYPE = "authentication.type"

  // driver
  val ENCRYPTION_ENABLED = "encryption.enabled"
  val ENCRYPTION_TRUST_STRATEGY = "encryption.trust.strategy"
  val ENCRYPTION_CA_CERTIFICATE_PATH = "encryption.ca.certificate.path"
  val CONNECTION_MAX_LIFETIME_MSECS = "connection.max.lifetime.msecs"
  val CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS = "connection.liveness.timeout.msecs"
  val CONNECTION_ACQUISITION_TIMEOUT_MSECS = "connection.acquisition.timeout.msecs"
  val CONNECTION_TIMEOUT_MSECS = "connection.timeout.msecs"
  val TRANSACTION_TIMEOUT_MSECS = "db.transaction.timeout"

  // session options
  val DATABASE = "database"
  val ACCESS_MODE = "access.mode"
  val SAVE_MODE = "save.mode"

  val PUSHDOWN_FILTERS_ENABLED = "pushdown.filters.enabled"
  val PUSHDOWN_COLUMNS_ENABLED = "pushdown.columns.enabled"
  val PUSHDOWN_AGGREGATE_ENABLED = "pushdown.aggregate.enabled"
  val PUSHDOWN_LIMIT_ENABLED = "pushdown.limit.enabled"
  val PUSHDOWN_TOPN_ENABLED = "pushdown.topN.enabled"

  // schema options
  val SCHEMA_STRATEGY = "schema.strategy"
  val SCHEMA_FLATTEN_LIMIT = "schema.flatten.limit"
  // deprecated in favor of...
  val SCHEMA_OPTIMIZATION_TYPE = "schema.optimization.type"
  // ...these options
  val SCHEMA_OPTIMIZATION = "schema.optimization"
  val SCHEMA_OPTIMIZATION_NODE_KEY = "schema.optimization.node.keys"
  val SCHEMA_OPTIMIZATION_RELATIONSHIP_KEY = "schema.optimization.relationship.keys"
  // map aggregation
  val SCHEMA_MAP_GROUP_DUPLICATE_KEYS = "schema.map.group.duplicate.keys"

  // index options
  val INDEX_AWAIT_TIMEOUT_SEC = "index.await.timeout"

  // partitions
  val PARTITIONS = "partitions"

  // orderBy
  val ORDER_BY = "orderBy"

  // skip.nulls postfix
  val SKIP_NULLS = "skip.nulls"

  // Node Metadata
  val NODE_KEYS = "node.keys"
  val NODE_KEYS_SKIP_NULLS = s"${NODE_KEYS}.${SKIP_NULLS}"
  val NODE_PROPS = "node.properties"

  val BATCH_SIZE = "batch.size"
  val SUPPORTED_SAVE_MODES = Seq(SaveMode.Overwrite, SaveMode.ErrorIfExists, SaveMode.Append)

  // Relationship Metadata
  val RELATIONSHIP_SOURCE_LABELS =
    s"${QueryType.RELATIONSHIP.toString.toLowerCase}.source.${QueryType.LABELS.toString.toLowerCase}"
  val RELATIONSHIP_SOURCE_NODE_KEYS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.source.$NODE_KEYS"
  val RELATIONSHIP_SOURCE_NODE_KEYS_SKIP_NULLS = s"${RELATIONSHIP_SOURCE_NODE_KEYS}.${SKIP_NULLS}"
  val RELATIONSHIP_SOURCE_NODE_PROPS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.source.$NODE_PROPS"
  val RELATIONSHIP_SOURCE_SAVE_MODE = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.source.$SAVE_MODE"

  val RELATIONSHIP_TARGET_LABELS =
    s"${QueryType.RELATIONSHIP.toString.toLowerCase}.target.${QueryType.LABELS.toString.toLowerCase}"
  val RELATIONSHIP_TARGET_NODE_KEYS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.target.$NODE_KEYS"
  val RELATIONSHIP_TARGET_NODE_KEYS_SKIP_NULLS = s"${RELATIONSHIP_TARGET_NODE_KEYS}.${SKIP_NULLS}"
  val RELATIONSHIP_TARGET_NODE_PROPS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.target.$NODE_PROPS"
  val RELATIONSHIP_TARGET_SAVE_MODE = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.target.$SAVE_MODE"
  val RELATIONSHIP_PROPERTIES = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.properties"
  val RELATIONSHIP_NODES_MAP = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.nodes.map"
  val RELATIONSHIP_SAVE_STRATEGY = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.save.strategy"
  val RELATIONSHIP_KEYS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.keys"
  val RELATIONSHIP_KEYS_SKIP_NULLS = s"${RELATIONSHIP_KEYS}.${SKIP_NULLS}"

  // Query metadata
  val QUERY_COUNT = "query.count"

  // Transaction Metadata
  val TRANSACTION_RETRIES = "transaction.retries"
  val TRANSACTION_RETRY_TIMEOUT = "transaction.retry.timeout"
  val TRANSACTION_CODES_FAIL = "transaction.codes.fail"

  // Streaming
  val STREAMING_PROPERTY_NAME = "streaming.property.name"
  val STREAMING_FROM = "streaming.from"
  val STREAMING_QUERY_OFFSET = "streaming.query.offset"

  val SCRIPT = "script"
  val SCRIPT_PREFIX = "script."

  // Data conversion
  val TYPE_CONVERSION = "type.conversion"

  // defaults
  val DEFAULT_EMPTY = ""
  val DEFAULT_TIMEOUT: Int = -1
  val DEFAULT_ACCESS_MODE = AccessMode.READ
  val DEFAULT_AUTH_TYPE = "basic"
  val DEFAULT_ENCRYPTION_ENABLED = false
  val DEFAULT_SCHEMA_FLATTEN_LIMIT = 10
  val DEFAULT_BATCH_SIZE = 5000
  val DEFAULT_TRANSACTION_RETRIES = 3
  val DEFAULT_TRANSACTION_RETRY_TIMEOUT = 0
  val DEFAULT_TRANSACTION_TIMEOUT = null
  val DEFAULT_RELATIONSHIP_NODES_MAP = false
  val DEFAULT_SCHEMA_STRATEGY = SchemaStrategy.SAMPLE
  val DEFAULT_SCHEMA_OPTIMIZATION_NODE_KEY = ConstraintsOptimizationType.NONE
  val DEFAULT_SCHEMA_OPTIMIZATION_RELATIONSHIP_KEY = ConstraintsOptimizationType.NONE
  val DEFAULT_SCHEMA_OPTIMIZATION = SchemaConstraintsOptimizationType.NONE
  val DEFAULT_RELATIONSHIP_SAVE_STRATEGY: RelationshipSaveStrategy.Value = RelationshipSaveStrategy.NATIVE
  val DEFAULT_RELATIONSHIP_SOURCE_SAVE_MODE: NodeSaveMode.Value = NodeSaveMode.Match
  val DEFAULT_RELATIONSHIP_TARGET_SAVE_MODE: NodeSaveMode.Value = NodeSaveMode.Match
  val DEFAULT_PUSHDOWN_FILTERS_ENABLED = true
  val DEFAULT_PUSHDOWN_COLUMNS_ENABLED = true
  val DEFAULT_PUSHDOWN_AGGREGATE_ENABLED = true
  val DEFAULT_PUSHDOWN_LIMIT_ENABLED = true
  val DEFAULT_PUSHDOWN_TOPN_ENABLED = true
  val DEFAULT_PARTITIONS = 1
  val DEFAULT_OPTIMIZATION_TYPE = OptimizationType.NONE
  val DEFAULT_SAVE_MODE = SaveMode.Overwrite
  val DEFAULT_STREAMING_FROM = StreamingFrom.NOW

  // Default values optimizations for Aura please look at: https://aura.support.neo4j.com/hc/en-us/articles/1500002493281-Neo4j-Java-driver-settings-for-Aura
  val DEFAULT_CONNECTION_MAX_LIFETIME_MSECS = Duration.ofMinutes(8).toMillis
  val DEFAULT_CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS = Duration.ofMinutes(2).toMillis

  val DEFAULT_MAP_GROUP_DUPLICATE_KEYS = false

  val DEFAULT_INDEX_AWAIT_TIMEOUT_SEC = 300

  var DEFAULT_AUTH_PARAMETERS: Map[String, String] =
    Seq("username", "password", "ticket", "principal", "credentials", "realm", "scheme", "token")
      .map(name => name -> DEFAULT_EMPTY).toMap

  private val DEFAULT_TYPE_CONVERSION = "default"

  def fromSession(sparkSession: Option[SparkSession], options: java.util.Map[String, String]): Neo4jOptions = {
    val sessionLevelOptions = sparkSession
      .map {
        _.conf
          .getAll
          .filterKeys(k => k.startsWith("neo4j."))
          .map { elem => (elem._1.substring("neo4j.".length), elem._2) }
          .toMap
      }
      .getOrElse(Map.empty)

    new Neo4jOptions((sessionLevelOptions ++ options.asScala).asJava)
  }
}

class CaseInsensitiveEnumeration extends Enumeration {

  def withCaseInsensitiveName(s: String): Value = {
    values.find(_.toString.toLowerCase() == s.toLowerCase).getOrElse(
      throw new NoSuchElementException(s"No value found for '$s'")
    )
  }
}

object StreamingFrom extends CaseInsensitiveEnumeration {
  val ALL, NOW = Value

  class StreamingFromValue(value: Value) {

    def value(): Long = value match {
      case ALL => -1L
      case NOW => System.currentTimeMillis()
    }
  }

  implicit def valToStreamingFromValue(value: Value): StreamingFromValue = new StreamingFromValue(value)
}

object StorageType extends CaseInsensitiveEnumeration {
  val NEO4J, SPARK = Value
}

object QueryType extends CaseInsensitiveEnumeration {
  val QUERY, LABELS, RELATIONSHIP, GDS = Value
}

object RelationshipSaveStrategy extends CaseInsensitiveEnumeration {
  val NATIVE, KEYS = Value
}

object NodeSaveMode extends CaseInsensitiveEnumeration {
  val Overwrite, ErrorIfExists, Match, Append = Value

  def fromSaveMode(saveMode: SaveMode): Value = {
    saveMode match {
      case SaveMode.Overwrite     => Overwrite
      case SaveMode.ErrorIfExists => ErrorIfExists
      case SaveMode.Append        => Append
      case _                      => throw new IllegalArgumentException(s"SaveMode $saveMode not supported")
    }
  }
}

object SchemaStrategy extends CaseInsensitiveEnumeration {
  val STRING, SAMPLE = Value
}

object OptimizationType extends CaseInsensitiveEnumeration {
  val INDEX, NODE_CONSTRAINTS, NONE = Value
}

object ConstraintsOptimizationType extends CaseInsensitiveEnumeration {
  val KEY, UNIQUE, NONE = Value
}

object SchemaConstraintsOptimizationType extends CaseInsensitiveEnumeration {
  val TYPE, EXISTS, NONE = Value
}
