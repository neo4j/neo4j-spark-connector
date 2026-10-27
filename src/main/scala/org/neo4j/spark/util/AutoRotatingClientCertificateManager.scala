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

import org.neo4j.driver.ClientCertificate
import org.neo4j.driver.ClientCertificateManager
import org.neo4j.driver.ClientCertificateManagers
import org.neo4j.driver.ClientCertificates

import java.io.File
import java.util.concurrent.CompletionStage

/**
 * Client certificate manager for mTLS that supports certificate rotation.
 *
 * It delegates to the driver's `RotatingClientCertificateManager` (see `ClientCertificateManagers.rotating`), so the
 * driver only reloads the certificate when it actually changed. Whenever a new connection is opened, the size and
 * modification time of the certificate and key files are checked; if either file changed (e.g. renewed by
 * cert-manager or Vault), the manager is rotated and every subsequent connection uses the new certificate without
 * restarting the job. Existing connections keep their certificate until they are closed.
 *
 * The files must be available at the same path on the Spark driver and on every executor.
 */
class AutoRotatingClientCertificateManager(certificatePath: String, keyPath: String, keyPassword: Option[String])
    extends ClientCertificateManager {

  import AutoRotatingClientCertificateManager.validateReadable

  private val certificateFile = new File(certificatePath)
  private val keyFile = new File(keyPath)

  validateReadable(Neo4jOptions.ENCRYPTION_CLIENT_CERTIFICATE_PATH, certificateFile)
  validateReadable(Neo4jOptions.ENCRYPTION_CLIENT_KEY_PATH, keyFile)

  // `ClientCertificate` only references the files, the driver (re)reads them every time the manager is rotated
  private val certificate = ClientCertificates.of(certificateFile, keyFile, keyPassword.orNull)
  private val delegate = ClientCertificateManagers.rotating(certificate)
  private var fingerprint = currentFingerprint()

  override def getClientCertificate: CompletionStage[ClientCertificate] = {
    synchronized {
      val latest = currentFingerprint()
      if (latest != fingerprint) {
        fingerprint = latest
        delegate.rotate(certificate)
      }
    }
    delegate.getClientCertificate
  }

  // a missing file yields zeros, so it is detected as a change once it reappears
  private def currentFingerprint(): (Long, Long, Long, Long) =
    (certificateFile.lastModified(), certificateFile.length(), keyFile.lastModified(), keyFile.length())
}

object AutoRotatingClientCertificateManager {

  private def validateReadable(option: String, file: File): Unit =
    require(file.isFile && file.canRead, s"'$option' must point to a readable file, but got '${file.getPath}'")
}
