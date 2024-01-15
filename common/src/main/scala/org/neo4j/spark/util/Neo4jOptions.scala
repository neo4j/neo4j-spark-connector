package org.neo4j.spark.util

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.neo4j.driver.Config.TrustStrategy
import org.neo4j.driver._
import org.neo4j.driver.net.{ServerAddress, ServerAddressResolver}
import org.neo4j.spark.util.Neo4jImplicits.StringMapImplicits

import java.io.File
import java.net.URI
import java.time.Duration
import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.language.implicitConversions


class Neo4jOptions(private val options: java.util.Map[String, String]) extends Serializable {
  import Neo4jOptions._
  import QueryType._

  def asMap() = new util.HashMap[String, String](options)

  private def parameters: util.Map[String, String] = {
    val sparkOptions = SparkSession.getActiveSession
      .map { _.conf
        .getAll
        .filterKeys(k => k.startsWith("neo4j."))
        .map { elem => (elem._1.substring("neo4.".length + 1), elem._2) }
        .toMap
      }
      .getOrElse(Map.empty)


    (sparkOptions ++ options.asScala).asJava
  }

  private def getRequiredParameter(parameter: String): String = {
    if (!parameters.containsKey(parameter) || parameters.get(parameter).isEmpty) {
      throw new IllegalArgumentException(s"Parameter '$parameter' is required")
    }

    parameters.get(parameter)
  }

  private def getParameter(parameter: String, defaultValue: String = ""): String = Some(parameters.getOrDefault(parameter, defaultValue))
    .flatMap(Option(_)) // to turn null into None
    .map(_.trim)
    .getOrElse(defaultValue)

  val saveMode: String = getParameter(SAVE_MODE, DEFAULT_SAVE_MODE.toString)
  val pushdownFiltersEnabled: Boolean = getParameter(PUSHDOWN_FILTERS_ENABLED, DEFAULT_PUSHDOWN_FILTERS_ENABLED.toString).toBoolean
  val pushdownColumnsEnabled: Boolean = getParameter(PUSHDOWN_COLUMNS_ENABLED, DEFAULT_PUSHDOWN_COLUMNS_ENABLED.toString).toBoolean
  val pushdownAggregateEnabled: Boolean = getParameter(PUSHDOWN_AGGREGATE_ENABLED, DEFAULT_PUSHDOWN_AGGREGATE_ENABLED.toString).toBoolean
  val pushdownLimitEnabled: Boolean = getParameter(PUSHDOWN_LIMIT_ENABLED, DEFAULT_PUSHDOWN_LIMIT_ENABLED.toString).toBoolean
  val pushdownTopNEnabled: Boolean = getParameter(PUSHDOWN_TOPN_ENABLED, DEFAULT_PUSHDOWN_TOPN_ENABLED.toString).toBoolean

  val schemaMetadata: Neo4jSchemaMetadata = Neo4jSchemaMetadata(getParameter(SCHEMA_FLATTEN_LIMIT, DEFAULT_SCHEMA_FLATTEN_LIMIT.toString).toInt,
    SchemaStrategy.withCaseInsensitiveName(getParameter(SCHEMA_STRATEGY, DEFAULT_SCHEMA_STRATEGY.toString).toUpperCase),
    OptimizationType.withCaseInsensitiveName(getParameter(SCHEMA_OPTIMIZATION_TYPE, DEFAULT_OPTIMIZATION_TYPE.toString).toUpperCase),
    getParameter(SCHEMA_MAP_GROUP_DUPLICATE_KEYS, DEFAULT_MAP_GROUP_DUPLICATE_KEYS.toString).toBoolean)

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
    case ("", "", "", gds) => Neo4jQueryOptions(GDS, gds)
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
    getParameter(AUTH_BASIC_USERNAME, DEFAULT_EMPTY),
    getParameter(AUTH_BASIC_PASSWORD, DEFAULT_EMPTY),
    getParameter(AUTH_KERBEROS_TICKET, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_PRINCIPAL, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_CREDENTIALS, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_REALM, DEFAULT_EMPTY),
    getParameter(AUTH_CUSTOM_SCHEME, DEFAULT_EMPTY),
    getParameter(AUTH_BEARER_TOKEN, DEFAULT_EMPTY),
    getParameter(ENCRYPTION_ENABLED, DEFAULT_ENCRYPTION_ENABLED.toString).toBoolean,
    Option(getParameter(ENCRYPTION_TRUST_STRATEGY, null)),
    getParameter(ENCRYPTION_CA_CERTIFICATE_PATH, DEFAULT_EMPTY),
    getParameter(CONNECTION_MAX_LIFETIME_MSECS, DEFAULT_CONNECTION_MAX_LIFETIME_MSECS.toString).toInt,
    getParameter(CONNECTION_ACQUISITION_TIMEOUT_MSECS, DEFAULT_TIMEOUT.toString).toInt,
    getParameter(CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS, DEFAULT_CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS.toString).toInt,
    getParameter(CONNECTION_TIMEOUT_MSECS, DEFAULT_TIMEOUT.toString).toInt
  )

  val session: Neo4jSessionOptions = Neo4jSessionOptions(
    getParameter(DATABASE, DEFAULT_EMPTY),
    AccessMode.valueOf(getParameter(ACCESS_MODE, DEFAULT_ACCESS_MODE.toString).toUpperCase())
  )

  val nodeMetadata: Neo4jNodeMetadata = initNeo4jNodeMetadata()

  private def mapPropsString(str: String): Map[String, String] = str.split(",")
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

  private def initNeo4jNodeMetadata(nodeKeysString: String = getParameter(NODE_KEYS, ""),
                                    labelsString: String = query.value,
                                    nodePropsString: String = ""): Neo4jNodeMetadata = {

    val nodeKeys = mapPropsString(nodeKeysString)
    val nodeProps = mapPropsString(nodePropsString)

    val labels = labelsString
      .split(":")
      .map(_.trim)
      .filter(_.nonEmpty)
    Neo4jNodeMetadata(labels, nodeKeys, nodeProps)
  }

  val transactionMetadata: Neo4jTransactionMetadata = initNeo4jTransactionMetadata()

  val script: Array[String] = getParameter(SCRIPT)
    .split(";")
    .map(_.trim)
    .filterNot(_.isEmpty)

  private def initNeo4jTransactionMetadata(): Neo4jTransactionMetadata = {
    val retries = getParameter(TRANSACTION_RETRIES, DEFAULT_TRANSACTION_RETRIES.toString).toInt
    val failOnTransactionCodes = getParameter(TRANSACTION_CODES_FAIL, DEFAULT_EMPTY)
      .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .toSet
    val batchSize = getParameter(BATCH_SIZE, DEFAULT_BATCH_SIZE.toString).toInt
    val retryTimeout = getParameter(TRANSACTION_RETRY_TIMEOUT, DEFAULT_TRANSACTION_RETRY_TIMEOUT.toString).toInt
    Neo4jTransactionMetadata(retries, failOnTransactionCodes, batchSize, retryTimeout)
  }

  val relationshipMetadata: Neo4jRelationshipMetadata = initNeo4jRelationshipMetadata()

  private def initNeo4jRelationshipMetadata(): Neo4jRelationshipMetadata = {
    val source = initNeo4jNodeMetadata(getParameter(RELATIONSHIP_SOURCE_NODE_KEYS, ""),
      getParameter(RELATIONSHIP_SOURCE_LABELS, ""),
      getParameter(RELATIONSHIP_SOURCE_NODE_PROPS, ""))

    val target = initNeo4jNodeMetadata(getParameter(RELATIONSHIP_TARGET_NODE_KEYS, ""),
      getParameter(RELATIONSHIP_TARGET_LABELS, ""),
      getParameter(RELATIONSHIP_TARGET_NODE_PROPS, ""))

    val nodeMap = getParameter(RELATIONSHIP_NODES_MAP, DEFAULT_RELATIONSHIP_NODES_MAP.toString).toBoolean

    val relProps = mapPropsString(getParameter(RELATIONSHIP_PROPERTIES))

    val writeStrategy = RelationshipSaveStrategy.withCaseInsensitiveName(getParameter(RELATIONSHIP_SAVE_STRATEGY, DEFAULT_RELATIONSHIP_SAVE_STRATEGY.toString).toUpperCase)
    val sourceSaveMode = NodeSaveMode.withCaseInsensitiveName(getParameter(RELATIONSHIP_SOURCE_SAVE_MODE, DEFAULT_RELATIONSHIP_SOURCE_SAVE_MODE.toString))
    val targetSaveMode = NodeSaveMode.withCaseInsensitiveName(getParameter(RELATIONSHIP_TARGET_SAVE_MODE, DEFAULT_RELATIONSHIP_TARGET_SAVE_MODE.toString))

    Neo4jRelationshipMetadata(source, target, sourceSaveMode, targetSaveMode, relProps, query.value, nodeMap, writeStrategy)
  }

  private def initNeo4jQueryMetadata(): Neo4jQueryMetadata = Neo4jQueryMetadata(
    query.value.trim, getParameter(QUERY_COUNT, "").trim
  )

  val queryMetadata: Neo4jQueryMetadata = initNeo4jQueryMetadata()

  private def initNeo4jGdsMetadata(): Neo4jGdsMetadata = Neo4jGdsMetadata(
    parameters.asScala
      .filterKeys(k => k.startsWith("gds."))
      .map(t => (t._1.substring("gds.".length), t._2))
      .toMap
      .toNestedJavaMap
  )

  val gdsMetadata: Neo4jGdsMetadata = initNeo4jGdsMetadata()

  val partitions: Int = getParameter(PARTITIONS, DEFAULT_PARTITIONS.toString).toInt

  val streamingOrderBy: String = getParameter(ORDER_BY, getParameter(STREAMING_PROPERTY_NAME))

  val apocConfig: Neo4jApocConfig = Neo4jApocConfig(parameters.asScala
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

  val streamingOptions: Neo4jStreamingOptions = Neo4jStreamingOptions(getParameter(STREAMING_PROPERTY_NAME),
    StreamingFrom.withCaseInsensitiveName(getParameter(STREAMING_FROM, DEFAULT_STREAMING_FROM.toString)),
    getParameter(STREAMING_QUERY_OFFSET),
    getParameter(STREAMING_CLEAN_STRUCT_TYPE_STORAGE, DEFAULT_STREAMING_CLEAN_STRUCT_TYPE_STORAGE.toString).toBoolean,
    StorageType.withCaseInsensitiveName(getParameter(STREAMING_METADATA_STORAGE, DEFAULT_STREAMING_METADATA_STORAGE.toString)))

}

case class Neo4jStreamingOptions(propertyName: String,
                                 from: StreamingFrom.Value,
                                 queryOffset: String,
                                 cleanStructTypeStorage: Boolean,
                                 storageType: StorageType.Value)

case class Neo4jApocConfig(procedureConfigMap: Map[String, AnyRef])

case class Neo4jSchemaMetadata(flattenLimit: Int,
                               strategy: SchemaStrategy.Value,
                               optimizationType: OptimizationType.Value,
                               mapGroupDuplicateKeys: Boolean)
case class Neo4jTransactionMetadata(retries: Int, failOnTransactionCodes: Set[String], batchSize: Int, retryTimeout: Long)

case class Neo4jNodeMetadata(labels: Seq[String], nodeKeys: Map[String, String], nodeProps: Map[String, String])
case class Neo4jRelationshipMetadata(
                                      source: Neo4jNodeMetadata,
                                      target: Neo4jNodeMetadata,
                                      sourceSaveMode: NodeSaveMode.Value,
                                      targetSaveMode: NodeSaveMode.Value,
                                      properties: Map[String, String],
                                      relationshipType: String,
                                      nodeMap: Boolean,
                                      saveStrategy: RelationshipSaveStrategy.Value
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
                               username: String,
                               password: String,
                               ticket: String,
                               principal: String,
                               credentials: String,
                               realm: String,
                               schema: String,
                               bearerToken: String,
                               encryption: Boolean,
                               trustStrategy: Option[String],
                               certificatePath: String,
                               lifetime: Int,
                               acquisitionTimeout: Int,
                               livenessCheckTimeout: Int,
                               connectionTimeout: Int
                             ) extends Serializable {

  def toDriverConfig: Config = {
    val builder = Config.builder()
      .withUserAgent(s"neo4j-${Neo4jUtil.connectorEnv}-connector/${Neo4jUtil.connectorVersion}")
      .withLogging(Logging.slf4j())

    if (lifetime > -1) builder.withMaxConnectionLifetime(lifetime, TimeUnit.MILLISECONDS)
    if (acquisitionTimeout > -1) builder.withConnectionAcquisitionTimeout(acquisitionTimeout, TimeUnit.MILLISECONDS)
    if (livenessCheckTimeout > -1) builder.withConnectionLivenessCheckTimeout(livenessCheckTimeout, TimeUnit.MILLISECONDS)
    if (connectionTimeout > -1) builder.withConnectionTimeout(connectionTimeout, TimeUnit.MILLISECONDS)

    val (primaryUrl, resolvers) = connectionUrls

    primaryUrl.getScheme match {
      case "neo4j+s" | "neo4j+ssc" | "bolt+s" | "bolt+ssc" => ()
      case _ => {
        if (!encryption) {
          builder.withoutEncryption()
        }
        else {
          builder.withEncryption()
        }
        trustStrategy
          .map(Config.TrustStrategy.Strategy.valueOf)
          .map {
            case TrustStrategy.Strategy.TRUST_ALL_CERTIFICATES => TrustStrategy.trustAllCertificates()
            case TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES => TrustStrategy.trustSystemCertificates()
            case TrustStrategy.Strategy.TRUST_CUSTOM_CA_SIGNED_CERTIFICATES => TrustStrategy.trustCustomCertificateSignedBy(new File(certificatePath))
          }.foreach(builder.withTrustStrategy)
      }
    }

    if (resolvers.nonEmpty) {
      builder.withResolver(new ServerAddressResolver {
        override def resolve(serverAddress: ServerAddress): util.Set[ServerAddress] = resolvers.asJava
      })
    }

    builder.build()
  }

  // public only for testing purposes
  def connectionUrls: (URI, Set[ServerAddress]) = {
    val urls = url.split(",").toList
    val extraUrls = urls
      .drop(1)
      .map(_.trim)
      .map(URI.create)
      .map(uri => ServerAddress.of(uri.getHost, if (uri.getPort > -1) uri.getPort else 7687))
      .toSet
    (URI.create(urls.head.trim), extraUrls)
  }

  // public only for testing purposes
  def toNeo4jAuth: AuthToken = {
    auth match {
      case "basic" => AuthTokens.basic(username, password)
      case "none" => AuthTokens.none()
      case "kerberos" => AuthTokens.kerberos(ticket)
      case "custom" => AuthTokens.custom(principal, credentials, realm, schema)
      case "bearer" => AuthTokens.bearer(bearerToken)
      case _ => throw new IllegalArgumentException(s"Authentication method '${auth}' is not supported")
    }
  }
}

object Neo4jOptions {

  // connection options
  val URL = "url"

  // auth
  val AUTH_TYPE = "authentication.type" // basic, none, kerberos, custom
  val AUTH_BASIC_USERNAME = "authentication.basic.username"
  val AUTH_BASIC_PASSWORD = "authentication.basic.password"
  val AUTH_KERBEROS_TICKET = "authentication.kerberos.ticket"
  val AUTH_CUSTOM_PRINCIPAL = "authentication.custom.principal"
  val AUTH_CUSTOM_CREDENTIALS = "authentication.custom.credentials"
  val AUTH_CUSTOM_REALM = "authentication.custom.realm"
  val AUTH_CUSTOM_SCHEME = "authentication.custom.scheme"
  val AUTH_BEARER_TOKEN = "authentication.bearer.token"

  // driver
  val ENCRYPTION_ENABLED = "encryption.enabled"
  val ENCRYPTION_TRUST_STRATEGY = "encryption.trust.strategy"
  val ENCRYPTION_CA_CERTIFICATE_PATH = "encryption.ca.certificate.path"
  val CONNECTION_MAX_LIFETIME_MSECS = "connection.max.lifetime.msecs"
  val CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS = "connection.liveness.timeout.msecs"
  val CONNECTION_ACQUISITION_TIMEOUT_MSECS = "connection.acquisition.timeout.msecs"
  val CONNECTION_TIMEOUT_MSECS = "connection.timeout.msecs"

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
  val SCHEMA_OPTIMIZATION_TYPE = "schema.optimization.type"
  // map aggregation
  val SCHEMA_MAP_GROUP_DUPLICATE_KEYS = "schema.map.group.duplicate.keys"

  // partitions
  val PARTITIONS = "partitions"

  // orderBy
  val ORDER_BY = "orderBy"

  // Node Metadata
  val NODE_KEYS = "node.keys"
  val NODE_PROPS = "node.properties"

  val BATCH_SIZE = "batch.size"
  val SUPPORTED_SAVE_MODES = Seq(SaveMode.Overwrite, SaveMode.ErrorIfExists, SaveMode.Append)

  // Relationship Metadata
  val RELATIONSHIP_SOURCE_LABELS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.source.${QueryType.LABELS.toString.toLowerCase}"
  val RELATIONSHIP_SOURCE_NODE_KEYS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.source.$NODE_KEYS"
  val RELATIONSHIP_SOURCE_NODE_PROPS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.source.$NODE_PROPS"
  val RELATIONSHIP_SOURCE_SAVE_MODE = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.source.$SAVE_MODE"
  val RELATIONSHIP_TARGET_LABELS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.target.${QueryType.LABELS.toString.toLowerCase}"
  val RELATIONSHIP_TARGET_NODE_KEYS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.target.$NODE_KEYS"
  val RELATIONSHIP_TARGET_NODE_PROPS = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.target.$NODE_PROPS"
  val RELATIONSHIP_TARGET_SAVE_MODE = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.target.$SAVE_MODE"
  val RELATIONSHIP_PROPERTIES = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.properties"
  val RELATIONSHIP_NODES_MAP = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.nodes.map"
  val RELATIONSHIP_SAVE_STRATEGY = s"${QueryType.RELATIONSHIP.toString.toLowerCase}.save.strategy"

  // Query metadata
  val QUERY_COUNT = "query.count"

  // Transaction Metadata
  val TRANSACTION_RETRIES = "transaction.retries"
  val TRANSACTION_RETRY_TIMEOUT = "transaction.retry.timeout"
  val TRANSACTION_CODES_FAIL = "transaction.codes.fail"

  // Streaming
  val STREAMING_PROPERTY_NAME = "streaming.property.name"
  val STREAMING_FROM = "streaming.from"
  val STREAMING_METADATA_STORAGE = "streaming.metadata.storage"
  val STREAMING_QUERY_OFFSET = "streaming.query.offset"
  val STREAMING_CLEAN_STRUCT_TYPE_STORAGE = "streaming.clean.struct-type.storage"

  val SCRIPT = "script"

  // defaults
  val DEFAULT_EMPTY = ""
  val DEFAULT_TIMEOUT: Int = -1
  val DEFAULT_ACCESS_MODE = AccessMode.READ
  val DEFAULT_AUTH_TYPE = "basic"
  val DEFAULT_ENCRYPTION_ENABLED = false
  val DEFAULT_ENCRYPTION_TRUST_STRATEGY = TrustStrategy.Strategy.TRUST_SYSTEM_CA_SIGNED_CERTIFICATES
  val DEFAULT_SCHEMA_FLATTEN_LIMIT = 10
  val DEFAULT_BATCH_SIZE = 5000
  val DEFAULT_TRANSACTION_RETRIES = 3
  val DEFAULT_TRANSACTION_RETRY_TIMEOUT = 0
  val DEFAULT_RELATIONSHIP_NODES_MAP = false
  val DEFAULT_SCHEMA_STRATEGY = SchemaStrategy.SAMPLE
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
  val DEFAULT_STREAMING_CLEAN_STRUCT_TYPE_STORAGE = false
  val DEFAULT_STREAMING_METADATA_STORAGE = StorageType.SPARK

  // Default values optimizations for Aura please look at: https://aura.support.neo4j.com/hc/en-us/articles/1500002493281-Neo4j-Java-driver-settings-for-Aura
  val DEFAULT_CONNECTION_MAX_LIFETIME_MSECS = Duration.ofMinutes(8).toMillis
  val DEFAULT_CONNECTION_LIVENESS_CHECK_TIMEOUT_MSECS = Duration.ofMinutes(2).toMillis

  val DEFAULT_MAP_GROUP_DUPLICATE_KEYS = false
}

class CaseInsensitiveEnumeration extends Enumeration {
  def withCaseInsensitiveName(s: String): Value = {
    values.find(_.toString.toLowerCase() == s.toLowerCase).getOrElse(
      throw new NoSuchElementException(s"No value found for '$s'"))
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
      case SaveMode.Overwrite => Overwrite
      case SaveMode.ErrorIfExists => ErrorIfExists
      case SaveMode.Append => Append
      case _ => throw new IllegalArgumentException(s"SaveMode $saveMode not supported")
    }
  }
}

object SchemaStrategy extends CaseInsensitiveEnumeration {
  val STRING, SAMPLE = Value
}

object OptimizationType extends CaseInsensitiveEnumeration {
  val INDEX, NODE_CONSTRAINTS, NONE = Value
}
