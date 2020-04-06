package org.neo4j.spark

import org.apache.spark.SparkConf
import org.neo4j.driver.{AccessMode, AuthToken, AuthTokens, Config, Driver, GraphDatabase, Session, SessionConfig}

/**
 * @author mh
 * @since 02.03.16
 */
case class Neo4jConfig(val url: String,
                       val user: String = "neo4j",
                       val password: Option[String] = None,
                       val database: Option[String] = None,
                       val encryption: Boolean) {

  private def boltConfig(): Config = if (encryption) Config.builder().withEncryption().build() else Config.builder().withoutEncryption().build()

  def driver(config: Neo4jConfig) : Driver = config.password match {
    case Some(pwd) => driver(config.url, AuthTokens.basic(config.user, pwd))
    case _ => driver(config.url, AuthTokens.none())
  }

  def driver(): Driver = driver(this)

  def driver(url: String, authToken: AuthToken): Driver = GraphDatabase.driver(url, authToken, boltConfig())

  def sessionConfig(write: Boolean = false): SessionConfig = database.map { SessionConfig.builder().withDatabase(_) }
    .getOrElse(SessionConfig.builder())
    .withDefaultAccessMode(if (write) AccessMode.WRITE else AccessMode.READ)
    .build()

}

object Neo4jConfig {
  val prefix = "spark.neo4j"
  val oldPrefix = "spark.neo4j.bolt"
  def apply(sparkConf: SparkConf): Neo4jConfig = {
    val url = sparkConf.getOption(s"$prefix.url")
      .getOrElse(sparkConf.get(s"$oldPrefix.url", "bolt://localhost"))
    val user = sparkConf.getOption(s"$prefix.user")
      .getOrElse(sparkConf.get(s"$oldPrefix.user", "neo4j"))
    val password: Option[String] = sparkConf.getOption(s"$prefix.password")
      .orElse(sparkConf.getOption(s"$oldPrefix.password"))
    val database: Option[String] = sparkConf.getOption(s"$prefix.database")
      .orElse(sparkConf.getOption(s"$oldPrefix.password"))
    val encryption: Boolean = sparkConf.getOption(s"$prefix.encryption")
        .map(bool => bool.toBoolean)
        .getOrElse(sparkConf.getBoolean(s"$oldPrefix.encryption", defaultValue = false))
    Neo4jConfig(url, user, password, database, encryption)
  }
}
