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

import dasniko.testcontainers.keycloak.KeycloakContainer
import org.junit.AfterClass
import org.junit.Assert.assertEquals
import org.junit.BeforeClass
import org.junit.Test
import org.neo4j.Neo4jContainerExtension
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.spark.ReauthenticationIT.KEYCLOAK
import org.neo4j.spark.ReauthenticationIT.NEO4J
import org.neo4j.spark.SparkConnectorScalaSuiteIT.ss
import org.testcontainers.containers.Network
import org.testcontainers.utility.MountableFile

object ReauthenticationIT {

  private val NETWORK = Network.newNetwork

  private val KEYCLOAK = new KeycloakContainer("quay.io/keycloak/keycloak:26.2.5")
    .withNetwork(NETWORK).withNetworkAliases("keycloak")
    .withCopyFileToContainer(
      MountableFile.forClasspathResource("/neo4j-keycloak.jks"),
      "/opt/keycloak/conf/server.keystore"
    )
    .withEnv("KC_HTTPS_KEY_STORE_FILE", "/opt/keycloak/conf/server.keystore")
    .withEnv("KC_HTTPS_KEY_STORE_PASSWORD", "testpwd")
    .withRealmImportFile("neo4j-sso-test-realm.json")
    .withEnv("KC_HOSTNAME", "https://keycloak:8443")
    .withEnv("KC_HOSTNAME_BACKCHANNEL_DYNAMIC", "true")

  private val NEO4J = new Neo4jContainerExtension()
    .withNetwork(NETWORK)
    .withEnv("NEO4J_ACCEPT_LICENSE_AGREEMENT", "yes")
    .withCopyFileToContainer(MountableFile.forClasspathResource("/neo4j-keycloak.jks"), "/tmp/keycloak.jks")
    .withNeo4jConfig(
      "server.jvm.additional",
      "-Djavax.net.ssl.keyStore=/tmp/keycloak.jks -Djavax.net.ssl.keyStorePassword=testpwd -Djavax.net.ssl.trustStore=/tmp/keycloak.jks -Djavax.net.ssl.trustStorePassword=testpwd"
    )
    .withNeo4jConfig("dbms.security.authentication_providers", "oidc-keycloak,native")
    .withNeo4jConfig("dbms.security.authorization_providers", "oidc-keycloak,native")
    .withNeo4jConfig("dbms.security.oidc.keycloak.display_name", "Keycloak")
    .withNeo4jConfig("dbms.security.oidc.keycloak.auth_flow", "pkce")
    .withNeo4jConfig(
      "dbms.security.oidc.keycloak.well_known_discovery_uri",
      "https://keycloak:8443/realms/neo4j-sso-test/.well-known/openid-configuration"
    )
    .withNeo4jConfig(
      "dbms.security.oidc.keycloak.params",
      "client_id=neo4j-commons-client;response_type=code;scope=openid email roles"
    )
    .withNeo4jConfig("dbms.security.oidc.keycloak.audience", "account")
    .withNeo4jConfig("dbms.security.oidc.keycloak.issuer", "https://keycloak:8443/realms/neo4j-sso-test")
    .withNeo4jConfig("dbms.security.oidc.keycloak.client_id", "neo4j-commons-client")
    .withNeo4jConfig("dbms.security.oidc.keycloak.claims.username", "preferred_username")
    .withNeo4jConfig("dbms.security.oidc.keycloak.claims.groups", "groups")
    .withNeo4jConfig("dbms.security.auth_cache_ttl", "1s")

  @BeforeClass
  def setUp(): Unit = {
    KEYCLOAK.start()
    NEO4J.start()
  }

  @AfterClass
  def tearDown() = {
    TestUtil.closeSafely(NEO4J)
    TestUtil.closeSafely(KEYCLOAK)
    TestUtil.closeSafely(NETWORK)
  }
}

class ReauthenticationIT extends SparkConnectorScalaSuiteIT {

  @Test
  def createAnInstanceOfReAuthDriver(): Unit = {
    val options = Map(
      "url" -> NEO4J.getBoltUrl,
      "authentication.type" -> "keycloak",
      "authentication.keycloak.username" -> "john-tester",
      "authentication.keycloak.password" -> "testerpwd",
      "authentication.keycloak.authServerUrl" ->
        s"http://${KEYCLOAK.getHost}:${KEYCLOAK.getHttpPort}",
      "authentication.keycloak.realm" -> "neo4j-sso-test",
      "authentication.keycloak.clientId" -> "neo4j-commons-client",
      "authentication.keycloak.clientSecret" -> "QNrSpbh0mxhnlYlI21UcBaz3Htb734vi"
    )

    var driver: Driver = null
    try {
      driver = GraphDatabase.driver(NEO4J.getBoltUrl, AuthTokens.basic("neo4j", NEO4J.getAdminPassword))
      driver.session().run(" CREATE (n:Test {field: 42}) CREATE (t:Test {field: 45})").consume()
    } finally {
      driver.close()
    }

    val df = ss.read.format(classOf[DataSource].getName)
      .options(options)
      .option("query", "MATCH (t:Test {field: 42}) RETURN t.field")
      .load()
      .toDF()
    assertEquals(42, df.first().getLong(0))

    Thread.sleep(4000)
    val df2 = ss.read.format(classOf[DataSource].getName)
      .options(options)
      .option("query", "MATCH (t:Test {field: 45}) RETURN t.field")
      .load()
      .toDF()
    assertEquals(45, df2.first().getLong(0))
  }

}
