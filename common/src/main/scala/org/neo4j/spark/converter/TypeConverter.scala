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
package org.neo4j.spark.converter

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.neo4j.driver.types.Entity
import org.neo4j.spark.converter.CypherToSparkTypeConverter.cleanTerms
import org.neo4j.spark.converter.CypherToSparkTypeConverter.durationType
import org.neo4j.spark.converter.CypherToSparkTypeConverter.pointType
import org.neo4j.spark.converter.CypherToSparkTypeConverter.timeType
import org.neo4j.spark.converter.SparkToCypherTypeConverter.mapping
import org.neo4j.spark.service.SchemaService.normalizedClassName
import org.neo4j.spark.util.Neo4jImplicits.EntityImplicits

import scala.collection.JavaConverters._

trait TypeConverter[SOURCE_TYPE, DESTINATION_TYPE] {

  def convert(sourceType: SOURCE_TYPE, value: Any = null): DESTINATION_TYPE

}

object CypherToSparkTypeConverter {
  def apply(): CypherToSparkTypeConverter = new CypherToSparkTypeConverter()

  private val cleanTerms: String = "Unmodifiable|Internal|Iso|2D|3D|Offset|Local|Zoned"

  val durationType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("type", DataTypes.StringType, false),
    DataTypes.createStructField("months", DataTypes.LongType, false),
    DataTypes.createStructField("days", DataTypes.LongType, false),
    DataTypes.createStructField("seconds", DataTypes.LongType, false),
    DataTypes.createStructField("nanoseconds", DataTypes.IntegerType, false),
    DataTypes.createStructField("value", DataTypes.StringType, false)
  ))

  val pointType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("type", DataTypes.StringType, false),
    DataTypes.createStructField("srid", DataTypes.IntegerType, false),
    DataTypes.createStructField("x", DataTypes.DoubleType, false),
    DataTypes.createStructField("y", DataTypes.DoubleType, false),
    DataTypes.createStructField("z", DataTypes.DoubleType, true)
  ))

  val timeType: DataType = DataTypes.createStructType(Array(
    DataTypes.createStructField("type", DataTypes.StringType, false),
    DataTypes.createStructField("value", DataTypes.StringType, false)
  ))
}

class CypherToSparkTypeConverter extends TypeConverter[String, DataType] {

  override def convert(sourceType: String, value: Any = null): DataType = sourceType
    .replaceAll(cleanTerms, "") match {
    case "Node" | "Relationship" => if (value != null) value.asInstanceOf[Entity].toStruct else DataTypes.NullType
    case "NodeArray" | "RelationshipArray" =>
      if (value != null) DataTypes.createArrayType(value.asInstanceOf[Entity].toStruct) else DataTypes.NullType
    case "Boolean"                                      => DataTypes.BooleanType
    case "Long"                                         => DataTypes.LongType
    case "Double"                                       => DataTypes.DoubleType
    case "Point"                                        => pointType
    case "DateTime" | "ZonedDateTime" | "LocalDateTime" => DataTypes.TimestampType
    case "Time"                                         => timeType
    case "Date"                                         => DataTypes.DateType
    case "Duration"                                     => durationType
    case "Map" => {
      val valueType = if (value == null) {
        DataTypes.NullType
      } else {
        val map = value.asInstanceOf[java.util.Map[String, AnyRef]].asScala
        val types = map.values
          .map(normalizedClassName)
          .toSet
        if (types.size == 1) convert(types.head, map.values.head) else DataTypes.StringType
      }
      DataTypes.createMapType(DataTypes.StringType, valueType)
    }
    case "Array" => {
      val valueType = if (value == null) {
        DataTypes.NullType
      } else {
        val list = value.asInstanceOf[java.util.List[AnyRef]].asScala
        val types = list
          .map(normalizedClassName)
          .toSet
        if (types.size == 1) convert(types.head, list.head) else DataTypes.StringType
      }
      DataTypes.createArrayType(valueType)
    }
    // These are from APOC
    case "StringArray"   => DataTypes.createArrayType(DataTypes.StringType)
    case "LongArray"     => DataTypes.createArrayType(DataTypes.LongType)
    case "DoubleArray"   => DataTypes.createArrayType(DataTypes.DoubleType)
    case "BooleanArray"  => DataTypes.createArrayType(DataTypes.BooleanType)
    case "PointArray"    => DataTypes.createArrayType(pointType)
    case "DateTimeArray" => DataTypes.createArrayType(DataTypes.TimestampType)
    case "TimeArray"     => DataTypes.createArrayType(timeType)
    case "DateArray"     => DataTypes.createArrayType(DataTypes.DateType)
    case "DurationArray" => DataTypes.createArrayType(durationType)
    // Default is String
    case _ => DataTypes.StringType
  }
}

object SparkToCypherTypeConverter {
  def apply(): SparkToCypherTypeConverter = new SparkToCypherTypeConverter()

  private val mapping: Map[DataType, String] = Map(
    DataTypes.BooleanType -> "BOOLEAN",
    DataTypes.StringType -> "STRING",
    DataTypes.IntegerType -> "INTEGER",
    DataTypes.LongType -> "INTEGER",
    DataTypes.FloatType -> "FLOAT",
    DataTypes.DoubleType -> "FLOAT",
    DataTypes.DateType -> "DATE",
    DataTypes.TimestampType -> "LOCAL DATETIME",
    durationType -> "DURATION",
    pointType -> "POINT",
    // Cypher graph entities do not allow null values in arrays
    DataTypes.createArrayType(DataTypes.BooleanType, false) -> "LIST<BOOLEAN NOT NULL>",
    DataTypes.createArrayType(DataTypes.StringType, false) -> "LIST<STRING NOT NULL>",
    DataTypes.createArrayType(DataTypes.IntegerType, false) -> "LIST<INTEGER NOT NULL>",
    DataTypes.createArrayType(DataTypes.LongType, false) -> "LIST<INTEGER NOT NULL>",
    DataTypes.createArrayType(DataTypes.FloatType, false) -> "LIST<FLOAT NOT NULL>",
    DataTypes.createArrayType(DataTypes.DoubleType, false) -> "LIST<FLOAT NOT NULL>",
    DataTypes.createArrayType(DataTypes.DateType, false) -> "LIST<DATE NOT NULL>",
    DataTypes.createArrayType(DataTypes.TimestampType, false) -> "LIST<LOCAL DATETIME NOT NULL>",
    DataTypes.createArrayType(DataTypes.TimestampType, true) -> "LIST<LOCAL DATETIME NOT NULL>",
    DataTypes.createArrayType(durationType, false) -> "LIST<DURATION NOT NULL>",
    DataTypes.createArrayType(pointType, false) -> "LIST<POINT NOT NULL>"
  )
}

class SparkToCypherTypeConverter extends TypeConverter[DataType, String] {
  override def convert(sourceType: DataType, value: Any): String = mapping(sourceType)
}
