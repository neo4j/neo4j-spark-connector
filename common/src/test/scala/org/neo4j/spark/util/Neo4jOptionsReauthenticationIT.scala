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

import dasniko.testcontainers.keycloak.KeycloakContainer
import org.junit.AfterClass
import org.junit.Assert
import org.junit.BeforeClass
import org.junit.Test
import org.neo4j.Neo4jContainerExtension
import org.neo4j.spark.SparkConnectorScalaSuiteIT
import org.neo4j.spark.TestUtil
import org.neo4j.spark.util.Neo4jOptionsReauthenticationIT.KEYCLOAK
import org.neo4j.spark.util.Neo4jOptionsReauthenticationIT.NEO4J
import org.testcontainers.containers.Network
import org.testcontainers.utility.MountableFile

object Neo4jOptionsReauthenticationIT {

  private val NETWORK = Network.newNetwork

  private val KEYCLOAK = new KeycloakContainer("quay.io/keycloak/keycloak:26.2.5")
    .withNetwork(NETWORK).withNetworkAliases("keycloak")
    .useTlsKeystore("/neo4j-keycloak.jks", "testpwd")
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

class Neo4jOptionsReauthenticationIT extends SparkConnectorScalaSuiteIT {

  @Test
  def createAnInstanceOfReAuthDriver(): Unit = {
    val options: java.util.Map[String, String] = new java.util.HashMap[String, String]()
    options.put(Neo4jOptions.URL, NEO4J.getBoltUrl)
    options.put(Neo4jOptions.AUTH_TYPE, "keycloak")
    options.put(s"${Neo4jOptions.AUTH}.keycloak.username", "john-tester")
    options.put(s"${Neo4jOptions.AUTH}.keycloak.password", "testerpwd")
    options.put(
      s"${Neo4jOptions.AUTH}.keycloak.authServerUrl",
      s"http://${KEYCLOAK.getHost}:${KEYCLOAK.getHttpPort}"
    )
    options.put(s"${Neo4jOptions.AUTH}.keycloak.realm", "neo4j-sso-test")
    options.put(s"${Neo4jOptions.AUTH}.keycloak.clientId", "neo4j-commons-client")
    options.put(s"${Neo4jOptions.AUTH}.keycloak.clientSecret", "QNrSpbh0mxhnlYlI21UcBaz3Htb734vi")
    val neo4jOptions: Neo4jOptions = new Neo4jOptions(options)

    val driver = neo4jOptions.connection.createDriver()
    driver.verifyConnectivity()
    val result = driver.session().run("CREATE (t:Test {field: 42}) RETURN t.field").single().get(0).asInt()
    Assert.assertEquals(42, result)

    Thread.sleep(4000)
    val result2 = driver.session().run("CREATE (t:Test {field: 45}) RETURN t.field").single().get(0).asInt()
    Assert.assertEquals(45, result2)
  }

}
