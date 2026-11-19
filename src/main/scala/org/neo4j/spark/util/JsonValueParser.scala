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

import com.google.gson.stream.JsonReader
import com.google.gson.stream.JsonToken
import com.google.gson.stream.JsonWriter

import java.io.IOException
import java.io.StringReader
import java.io.StringWriter

/**
 * Parses option values as JSON into Java values; input that is not valid JSON is returned unchanged.
 *
 * Integers become the smallest fitting `Integer`, `Long` or `BigInteger`, other numbers become `Double`.
 *
 * Gson's lenient mode is never used: it accepts comments and unquoted strings, turning `abc#123` into `abc`.
 */
object JsonValueParser {

  /** Parses `raw` as JSON. Single-quoted strings are accepted in addition to double-quoted ones. */
  def parse(raw: String): Any = parseOrRaw(raw)(normalizeSingleQuotes(raw))(readValue)

  /** Parses `raw` as JSON, returning arrays and objects as compact JSON text. */
  def parsePrimitive(raw: String): Any = parseOrRaw(raw)(raw) { reader =>
    reader.peek() match {
      case JsonToken.BEGIN_ARRAY | JsonToken.BEGIN_OBJECT =>
        val out = new StringWriter()
        copyValue(reader, new JsonWriter(out))
        out.toString
      case _ => readValue(reader)
    }
  }

  /** Reads one JSON value spanning all of `json`, or returns `raw` if it is invalid or has trailing content. */
  private def parseOrRaw(raw: String)(json: String)(read: JsonReader => Any): Any =
    try {
      val reader = new JsonReader(new StringReader(json))
      val value = read(reader)
      if (reader.peek() != JsonToken.END_DOCUMENT) raw else value
    } catch {
      case _: IOException | _: IllegalStateException | _: NumberFormatException => raw
    }

  private def readValue(reader: JsonReader): Any = reader.peek() match {
    case JsonToken.NULL =>
      reader.nextNull()
      null
    case JsonToken.BOOLEAN => java.lang.Boolean.valueOf(reader.nextBoolean())
    case JsonToken.STRING  => reader.nextString()
    case JsonToken.NUMBER  => toNumber(reader.nextString())
    case JsonToken.BEGIN_ARRAY =>
      val list = new java.util.ArrayList[Any]()
      reader.beginArray()
      while (reader.hasNext) list.add(readValue(reader))
      reader.endArray()
      list
    case JsonToken.BEGIN_OBJECT =>
      val map = new java.util.LinkedHashMap[String, Any]()
      reader.beginObject()
      while (reader.hasNext) map.put(reader.nextName(), readValue(reader))
      reader.endObject()
      map
    case token => throw new IllegalStateException(s"Unexpected $token $reader")
  }

  private def toNumber(text: String): Any =
    if (text.exists(c => c == '.' || c == 'e' || c == 'E')) {
      java.lang.Double.valueOf(text)
    } else {
      val value = new java.math.BigInteger(text)
      if (value.bitLength() < 32) java.lang.Integer.valueOf(value.intValue())
      else if (value.bitLength() < 64) java.lang.Long.valueOf(value.longValue())
      else value
    }

  /** Copies one value, keeping number literals exactly as written. */
  private def copyValue(reader: JsonReader, writer: JsonWriter): Unit = reader.peek() match {
    case JsonToken.NULL =>
      reader.nextNull()
      writer.nullValue()
    case JsonToken.BOOLEAN => writer.value(reader.nextBoolean())
    case JsonToken.STRING  => writer.value(reader.nextString())
    case JsonToken.NUMBER  => writer.jsonValue(reader.nextString())
    case JsonToken.BEGIN_ARRAY =>
      reader.beginArray()
      writer.beginArray()
      while (reader.hasNext) copyValue(reader, writer)
      reader.endArray()
      writer.endArray()
    case JsonToken.BEGIN_OBJECT =>
      reader.beginObject()
      writer.beginObject()
      while (reader.hasNext) {
        writer.name(reader.nextName())
        copyValue(reader, writer)
      }
      reader.endObject()
      writer.endObject()
    case token => throw new IllegalStateException(s"Unexpected $token $reader")
  }

  /**
   * Rewrites single-quoted strings as double-quoted ones, since the strict reader rejects single quotes.
   * Inside them, `\'` becomes `'` and `"` becomes `\"`; everything else is copied unchanged.
   */
  private[util] def normalizeSingleQuotes(raw: String): String = {
    if (raw.indexOf('\'') < 0) {
      return raw
    }
    val sb = new java.lang.StringBuilder(raw.length + 8)
    var quote: Char = 0 // 0 when outside a string, otherwise the opening quote character
    var i = 0
    while (i < raw.length) {
      val c = raw.charAt(i)
      if (quote == 0) {
        if (c == '\'' || c == '"') {
          quote = c
          sb.append('"')
        } else {
          sb.append(c)
        }
      } else if (c == '\\' && i + 1 < raw.length) {
        val next = raw.charAt(i + 1)
        if (quote == '\'' && next == '\'') {
          sb.append('\'')
        } else {
          sb.append(c).append(next)
        }
        i += 1
      } else if (c == quote) {
        quote = 0
        sb.append('"')
      } else if (c == '"') {
        // only reachable inside a single-quoted string
        sb.append("\\\"")
      } else {
        sb.append(c)
      }
      i += 1
    }
    sb.toString
  }
}
