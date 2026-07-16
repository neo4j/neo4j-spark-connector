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

import org.neo4j.cypherdsl.core._
import org.neo4j.cypherdsl.core.ast.Visitable
import org.neo4j.cypherdsl.core.renderer.Configuration
import org.neo4j.cypherdsl.core.renderer.GeneralizedRenderer
import org.neo4j.cypherdsl.core.renderer.Renderer
import org.neo4j.cypherdsl.parser.CypherParser
import org.neo4j.cypherdsl.parser.ExpressionCreatedEventType.ON_RETURN_ITEM
import org.neo4j.cypherdsl.parser.Options
import org.neo4j.spark.cypher.AutomaticAliaser.defaultRenderer

import scala.jdk.CollectionConverters.CollectionHasAsScala

private[cypher] class AutomaticAliaser(private val fragmentRenderer: GeneralizedRenderer = defaultRenderer()) {

  /**
   * This aliases the results of a query if necessary.
   * For instance, <pre><code>MATCH (n) RETURN n.foo</pre></code>
   * gets replaced with
   * <pre><code>MATCH (n) RETURN n.foo AS &#96;n.foo&#96;</pre></code>
   * @param query the original query
   * @return the same query, with aliased returned columns if necessary
   */
  def aliasResults(query: String): AliasedQuery = {
    val returnFields = new ReturnFields()
    val statement: Visitable = CypherParser.parse(query, renderingOptions(returnFields))
    AliasedQuery(fragmentRenderer.render(statement), returnFields.fields)
  }

  private def renderingOptions(returnFields: ReturnFields) =
    Options.newOptions
      .withCallback(
        ON_RETURN_ITEM,
        classOf[Expression],
        alias _
      )
      // parse & extract return fields, last return clause wins (ie overwrites ReturnFields)
      .withReturnClauseFactory(returnDefinition => {
        returnFields.update(returnDefinition.getExpressions.asScala.toSeq)
        Clauses.returning(
          returnDefinition.isDistinct,
          returnDefinition.getExpressions,
          returnDefinition.getOptionalSortItems,
          returnDefinition.getOptionalSkip,
          returnDefinition.getOptionalLimit
        )
      })
      .build

  private def alias(expression: Expression): Expression =
    expression match {
      case asterisk: Asterisk                     => asterisk
      case literal: Literal[_]                    => literal.as(fragmentRenderer.render(literal))
      case param: Parameter[_]                    => param.as(fragmentRenderer.render(param))
      case listExpression: ListExpression         => listExpression.as(fragmentRenderer.render(listExpression))
      case mapExpression: MapExpression           => mapExpression.as(fragmentRenderer.render(mapExpression))
      case functionInvocation: FunctionInvocation => functionInvocation.as(fragmentRenderer.render(functionInvocation))
      case listAccess: ListOperator               => listAccess.as(fragmentRenderer.render(listAccess))
      case property: Property                     => property.as(fragmentRenderer.render(property))
      case _                                      => expression
    }

  private class ReturnFields {
    private var names: Seq[String] = Seq.empty

    def fields: Seq[String] = names

    def update(expressions: Seq[Expression]): Unit = {
      names = expressions.map(returnFieldName)
    }

    private def returnFieldName(expression: Expression): String =
      expression match {
        case _: Asterisk                => "*"
        case aliased: AliasedExpression => aliased.getAlias
        case name: SymbolicName         => name.getValue
        case other                      => fragmentRenderer.render(other)
      }
  }
}

private[cypher] case class AliasedQuery(query: String, returnedFields: Seq[String])

private object AutomaticAliaser {

  def defaultRenderer(): GeneralizedRenderer = {
    val config = Configuration.newConfig()
      .alwaysEscapeNames(false)
      .build()
    Renderer.getRenderer(config, classOf[GeneralizedRenderer])
  }
}
