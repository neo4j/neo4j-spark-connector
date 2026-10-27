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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.driver.ClientCertificate
import org.neo4j.driver.ClientCertificates

import java.io.File
import java.nio.file.Files
import java.nio.file.Path

class AutoRotatingClientCertificateManagerTest {

  @TempDir
  var dir: Path = _

  private var certFile: File = _
  private var keyFile: File = _

  @BeforeEach
  def setUp(): Unit = {
    certFile = Files.writeString(dir.resolve("client.pem"), "cert").toFile
    keyFile = Files.writeString(dir.resolve("client.key"), "key").toFile
    certFile.setLastModified(1000L)
    keyFile.setLastModified(1000L)
  }

  private def manager(password: Option[String] = None) =
    new AutoRotatingClientCertificateManager(certFile.getPath, keyFile.getPath, password)

  private def  next(manager: AutoRotatingClientCertificateManager): ClientCertificate =
    manager.getClientCertificate.toCompletableFuture.get()

  @Test
  def returns_initial_certificate_on_first_call(): Unit = {
    assertThat(next(manager())).isEqualTo(ClientCertificates.of(certFile, keyFile))
  }

  @Test
  def passes_key_password_to_certificate(): Unit = {
    assertThat(next(manager(Some("secret")))).isEqualTo(ClientCertificates.of(certFile, keyFile, "secret"))
  }

  @Test
  def signals_no_update_while_files_are_unchanged(): Unit = {
    val manager = this.manager()
    next(manager)

    assertThat(next(manager)).isNull()
    assertThat(next(manager)).isNull()
  }

  @Test
  def rotates_when_certificate_file_is_modified(): Unit = {
    val manager = this.manager()
    next(manager)

    certFile.setLastModified(2000L)

    assertThat(next(manager)).isEqualTo(ClientCertificates.of(certFile, keyFile))
    assertThat(next(manager)).isNull()
  }

  @Test
  def rotates_when_key_file_is_modified(): Unit = {
    val manager = this.manager()
    next(manager)

    keyFile.setLastModified(2000L)

    assertThat(next(manager)).isEqualTo(ClientCertificates.of(certFile, keyFile))
    assertThat(next(manager)).isNull()
  }

  @Test
  def rotates_when_file_size_changes_with_same_modification_time(): Unit = {
    val manager = this.manager()
    next(manager)

    Files.writeString(certFile.toPath, "renewed cert")
    certFile.setLastModified(1000L)

    assertThat(next(manager)).isNotNull
  }

  @Test
  def rotates_once_a_missing_file_reappears(): Unit = {
    val manager = this.manager()
    next(manager)

    assertThat(keyFile.delete()).isTrue
    // the driver fails to load the missing file and reports it on the new connection
    assertThat(next(manager)).isNotNull
    assertThat(next(manager)).isNull()

    Files.writeString(keyFile.toPath, "key")

    assertThat(next(manager)).isNotNull
  }

  @Test
  def fails_when_certificate_file_is_missing(): Unit = {
    assertThat(certFile.delete()).isTrue

    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => manager())
      .withMessageContaining(Neo4jOptions.ENCRYPTION_CLIENT_CERTIFICATE_PATH)
      .withMessageContaining(certFile.getPath)
  }

  @Test
  def fails_when_key_file_is_missing(): Unit = {
    assertThat(keyFile.delete()).isTrue

    assertThatExceptionOfType(classOf[IllegalArgumentException])
      .isThrownBy(() => manager())
      .withMessageContaining(Neo4jOptions.ENCRYPTION_CLIENT_KEY_PATH)
      .withMessageContaining(keyFile.getPath)
  }
}
