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
package org.neo4j.spark.auth

import org.neo4j.connectors.authn.AuthenticationToken
import org.neo4j.connectors.authn.AuthenticationTokenSupplierFactory

import java.util
import java.util.function.Supplier

class NoneAuthenticationSupplierFactory extends AuthenticationTokenSupplierFactory {

  override def getName: String = "none"

  override def create(
    username: String,
    password: String,
    parameters: util.Map[String, String]
  ): Supplier[AuthenticationToken] = () => AuthenticationToken.none()
}
