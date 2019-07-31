package org.neo4j.spark

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.v1._
import org.neo4j.driver.v1.summary.ResultSummary
import org.neo4j.driver.v1.types.{Type, TypeSystem}

import scala.collection.JavaConverters._

object Neo4jDataFrame {

  def mergeEdgeList(sc: SparkContext,
                    dataFrame: DataFrame,
                    source: (String, Seq[String]),
                    relationship: (String, Seq[String]),
                    target: (String, Seq[String]),
                    renamedColumns: Map[String, String] = Map.empty): Unit = {

    val sourceLabel: String = renamedColumns.getOrElse(source._2.head, source._2.head)
    val targetLabel: String = renamedColumns.getOrElse(target._2.head, target._2.head)
    val mergeStatement =
      s"""
        UNWIND {rows} as row
        MERGE (source:`${source._1}` {`${sourceLabel}` : row.source.`${sourceLabel}`}) ON CREATE SET source += row.source
        MERGE (target:`${target._1}` {`${targetLabel}` : row.target.`${targetLabel}`}) ON CREATE SET target += row.target
        MERGE (source)-[rel:`${relationship._1}`]->(target) ON CREATE SET rel += row.relationship
        """
    val partitions = Math.max(1, (dataFrame.count() / 10000).asInstanceOf[Int])
    val config = Neo4jConfig(sc.getConf)
    dataFrame.repartition(partitions).foreachPartition(rows => {
      val params: AnyRef = rows.map(r =>
        Map(
          "source" -> source._2.map(c => (renamedColumns.getOrElse(c, c), r.getAs[AnyRef](c))).toMap.asJava,
          "target" -> target._2.map(c => (renamedColumns.getOrElse(c, c), r.getAs[AnyRef](c))).toMap.asJava,
          "relationship" -> relationship._2.map(c => (c, r.getAs[AnyRef](c))).toMap.asJava)
          .asJava).asJava
      execute(config, mergeStatement, Map("rows" -> params).asJava, write = true)
    })
  }

  def createNodes(sc: SparkContext, dataFrame: DataFrame, nodes: (String, Seq[String]), renamedColumns: Map[String, String] = Map.empty): Unit = {

    val nodeLabel: String = renamedColumns.getOrElse(nodes._2.head, nodes._2.head)
    val createStatement =
      s"""
        UNWIND {rows} as row
        CREATE (node:`${nodes._1}` {`${nodeLabel}` : row.source.`${nodeLabel}`})
        SET node = row.node_properties
        """
    val partitions = Math.max(1, (dataFrame.count() / 10000).asInstanceOf[Int])
    val config = Neo4jConfig(sc.getConf)
    dataFrame.repartition(partitions).foreachPartition(rows => {
      val params: AnyRef = rows.map(r =>
        Map(
          "node_properties" -> nodes._2.map(c => (renamedColumns.getOrElse(c, c), r.getAs[AnyRef](c))).toMap.asJava)
          .asJava).asJava
      Neo4jDataFrame.execute(config, createStatement, Map("rows" -> params).asJava, write = true)
    })
  }

  def execute(config: Neo4jConfig, query: String, parameters: java.util.Map[String, AnyRef], write: Boolean = false): ResultSummary = {
    val driver: Driver = config.driver()
    val session = driver.session()
    try {
      val runner = new TransactionWork[ResultSummary]() {
        override def execute(tx: Transaction): ResultSummary =
          tx.run(query, parameters).consume()
      }
      if (write) {
        session.writeTransaction(runner)
      }
      else
        session.readTransaction(runner)
    }
    finally {
      if (session.isOpen) session.close()
      driver.close()
    }
  }

  def withDataType(sqlContext: SQLContext, query: String, parameters: Seq[(String, Any)], schema: (String, DataType)*) = {
    val rowRdd = Neo4jRowRDD(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, CypherTypes.schemaFromDataType(schema))
  }

  def apply(sqlContext: SQLContext, query: String, parameters: Seq[(String, Any)], schema: (String, String)*) = {
    val rowRdd = Neo4jRowRDD(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, CypherTypes.schemaFromNamedType(schema))
  }

  def apply(sqlContext: SQLContext, query: String, parameters: java.util.Map[String, AnyRef], write: Boolean = false): DataFrame = {
    val config = Neo4jConfig(sqlContext.sparkContext.getConf)
    val driver: Driver = config.driver()
    val session = driver.session()
    try {
      val runTransaction = new TransactionWork[DataFrame]() {
        override def execute(tx: Transaction): DataFrame = {
          val result = session.run(query, parameters)
          if (!result.hasNext) throw new RuntimeException("Can't determine schema from empty result")
          val peek: Record = result.peek()
          val fields = peek.keys().asScala.map(k => (k, peek.get(k).`type`())).map(keyType => CypherTypes.field(keyType))
          val schema = StructType(fields)
          val rowRdd = new Neo4jResultRdd(sqlContext.sparkContext, result.asScala, peek.size(), session, driver)
          sqlContext.createDataFrame(rowRdd, schema)
        }
      }
      if (write)
        session.writeTransaction(runTransaction)
      else
        session.readTransaction(runTransaction)
    }
    finally {
      if (session.isOpen) session.close()
      driver.close()
    }
  }


  class Neo4jResultRdd(@transient sc: SparkContext, result: Iterator[Record],
                       keyCount: Int, session: Session, driver: Driver) extends RDD[Row](sc, Nil) {

    def convert(value: AnyRef) = value match {
      case m: java.util.Map[_, _] => m.asScala
      case _                      => value
    }

    override def compute(split: Partition, context: TaskContext): Iterator[Row] =
      result.map { record =>
        val res = keyCount match {
          case 0 => Row.empty
          case 1 => Row(convert(record.get(0).asObject()))
          case c => Row.fromSeq((0 until c).map(i => convert(record.get(i).asObject())))
        }

        if (!result.hasNext) {
          session.close()
          driver.close()
        }

        res
      }

    override protected def getPartitions: Array[Partition] = Array(new Neo4jPartition())
  }

}

object CypherTypes {
  val INTEGER = DataTypes.LongType
  val FlOAT = DataTypes.DoubleType
  val STRING = DataTypes.StringType
  val BOOLEAN = DataTypes.BooleanType
  val NULL = DataTypes.NullType
  val DATETIME = DataTypes.TimestampType
  val DATE = DataTypes.DateType

  def typeOf(typ: String): DataType = typ.toUpperCase match {
    case "LONG"     => INTEGER
    case "INT"      => INTEGER
    case "INTEGER"  => INTEGER
    case "FLOAT"    => FlOAT
    case "DOUBLE"   => FlOAT
    case "NUMERIC"  => FlOAT
    case "STRING"   => STRING
    case "BOOLEAN"  => BOOLEAN
    case "BOOL"     => BOOLEAN
    case "DATETIME" => DATETIME
    case "DATE"     => DATE
    case "NULL"     => NULL
    case _          => STRING
  }

  def apply(typ: String): DataType =
    (typ.charAt(0), typ.substring(1, typ.length - 1), typ.charAt(typ.length - 1)) match {
      case ('[', inner, ']') => DataTypes.createArrayType(typeOf(inner), true)
      case ('{', inner, '}') => DataTypes.createMapType(STRING, typeOf(inner), true)
      case _                 => typeOf(typ)
    }

  //  val MAP = edges.MapType(edges.StringType,edges.AnyDataType)
  //  val LIST = edges.ArrayType(edges.AnyDataType)
  // , MAP, LIST, NODE, RELATIONSHIP, PATH


  def toSparkType(typeSystem: TypeSystem, typ: Type): org.apache.spark.sql.types.DataType =
    if (typ == typeSystem.BOOLEAN()) CypherTypes.BOOLEAN
    else if (typ == typeSystem.STRING()) CypherTypes.STRING
    else if (typ == typeSystem.INTEGER()) CypherTypes.INTEGER
    else if (typ == typeSystem.FLOAT()) CypherTypes.FlOAT
    else if (typ == typeSystem.NULL()) CypherTypes.NULL
    else CypherTypes.STRING

  //    type match {
  //    case typeSystem.MAP => edges.MapType(edges.StringType,edges.ObjectType)
  //    case typeSystem.LIST => edges.ArrayType(edges.ObjectType, containsNull = false)
  //    case typeSystem.NODE => edges.VertexType
  //    case typeSystem.RELATIONSHIP => edges.EdgeType
  //    case typeSystem.PATH => edges.GraphType
  //    case _ => edges.StringType
  //  }

  def field(keyType: (String, Type)): StructField = {
    StructField(keyType._1, CypherTypes.toSparkType(InternalTypeSystem.TYPE_SYSTEM, keyType._2))
  }

  def schemaFromNamedType(schemaInfo: Seq[(String, String)]) = {
    val fields = schemaInfo.map(field =>
      StructField(field._1, CypherTypes(field._2), nullable = true))
    StructType(fields)
  }

  def schemaFromDataType(schemaInfo: Seq[(String, DataType)]) = {
    val fields = schemaInfo.map(field =>
      StructField(field._1, field._2, nullable = true))
    StructType(fields)
  }

}
