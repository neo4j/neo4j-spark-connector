package org.neo4j.spark.streaming

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.connector.read.streaming.Offset
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.neo4j.spark.streaming.Neo4jOffset.mapper
import org.neo4j.spark.util.{Neo4jOptions, StreamingFrom}

import java.io.InputStream


object Neo4jOffset {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true)

  def from(json: String): Neo4jOffset = mapper.readValue(json, classOf[Neo4jOffset])

  def from(in: InputStream): Neo4jOffset = mapper.readValue(in, classOf[Neo4jOffset])

  def from(options: Neo4jOptions): Neo4jOffset = Neo4jOffset(options.streamingOptions.from.value())

  def fromOffset(value: Any): Neo4jOffset = value match {
    case l: Long => Neo4jOffset(l)
    case _ => throw new IllegalArgumentException(s"${value.getClass} not supported")
  }

  def now(dataType: DataType = DataTypes.LongType): Neo4jOffset = dataType match {
    case DataTypes.LongType => Neo4jOffset(System.currentTimeMillis())
  }

}

case class Neo4jOffset(value: Any, dataType: DataType = DataTypes.LongType) extends Offset with Serializable {

  def getValueAs[T]: T = value.asInstanceOf[T]

  override def json(): String = s"""{"dataType": ${dataType.json}, "value": ${mapper.writeValueAsString(value)}}"""
}
