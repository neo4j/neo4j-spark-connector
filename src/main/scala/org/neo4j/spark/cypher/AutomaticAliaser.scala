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
package org.neo4j.spark.cypher

import org.neo4j.cypherdsl.core.Expression
import org.neo4j.cypherdsl.core.FunctionInvocation
import org.neo4j.cypherdsl.core.ListExpression
import org.neo4j.cypherdsl.core.ListOperator
import org.neo4j.cypherdsl.core.Literal
import org.neo4j.cypherdsl.core.MapExpression
import org.neo4j.cypherdsl.core.Parameter
import org.neo4j.cypherdsl.core.Property
import org.neo4j.cypherdsl.core.ast.Visitable
import org.neo4j.cypherdsl.core.renderer.Configuration
import org.neo4j.cypherdsl.core.renderer.GeneralizedRenderer
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.cypherdsl.parser.CypherParser
import org.neo4j.cypherdsl.parser.ExpressionCreatedEventType.ON_RETURN_ITEM
import org.neo4j.cypherdsl.parser.Options
import org.neo4j.spark.cypher.AutomaticAliaser.defaultRenderer

class AutomaticAliaser(private val fragmentRenderer: GeneralizedRenderer = defaultRenderer()) {

  def aliasResults(query: String): String = {
    val statement: Visitable = CypherParser.parse(query, renderingOptions())
    fragmentRenderer.render(statement)
  }

  private def renderingOptions() =
    Options.newOptions.withCallback(
      ON_RETURN_ITEM,
      classOf[Expression],
      alias _
    ).build

  private def alias(expression: Expression): Expression =
    expression match {
      case literal: Literal[_]                    => literal.as(fragmentRenderer.render(literal))
      case param: Parameter[_]                    => param.as(fragmentRenderer.render(param))
      case listExpression: ListExpression         => listExpression.as(fragmentRenderer.render(listExpression))
      case mapExpression: MapExpression           => mapExpression.as(fragmentRenderer.render(mapExpression))
      case functionInvocation: FunctionInvocation => functionInvocation.as(fragmentRenderer.render(functionInvocation))
      case listAccess: ListOperator               => listAccess.as(fragmentRenderer.render(listAccess))
      case property: Property                     => property.as(fragmentRenderer.render(property))
      case _                                      => expression
    }
}

private object AutomaticAliaser {

  def defaultRenderer(): GeneralizedRenderer = {
    val config = Configuration.newConfig()
      .alwaysEscapeNames(false)
      .build()
    Renderer.getRenderer(config, classOf[GeneralizedRenderer])
  }
}
