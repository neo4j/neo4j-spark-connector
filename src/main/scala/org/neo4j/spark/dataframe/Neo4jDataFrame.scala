package org.neo4j.spark.dataframe

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.neo4j.driver._
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.types.{Type, TypeSystem}
import org.neo4j.spark.Neo4jConfig
import org.neo4j.spark.cypher.CypherHelpers._
import org.neo4j.spark.rdd.{Neo4jPartition, Neo4jRowRDD}
import org.neo4j.spark.utils.{Neo4jSessionAwareIterator, Neo4jUtils}
import org.neo4j.spark.utils.Neo4jUtils._

import scala.collection.JavaConverters._

object Neo4jDataFrame {

  def mergeEdgeList(sc: SparkContext,
                    dataFrame: DataFrame,
                    source: (String, Seq[String]),
                    relationship: (String, Seq[String]),
                    target: (String, Seq[String]),
                    renamedColumns: Map[String, String] = Map.empty,
                    partitions: Int = 1,
                    unwindBatchSize: Int = 10000): Unit = {

    createNodes(sc, dataFrame, source, renamedColumns, partitions, unwindBatchSize, true)
    createNodes(sc, dataFrame, target, renamedColumns, partitions, unwindBatchSize, true)

    val sourceKey: String = renamedColumns.getOrElse(source._2.head, source._2.head).quote
    val targetKey: String = renamedColumns.getOrElse(target._2.head, target._2.head).quote
    val relStatement = s"""|
        |UNWIND $$rows as row
        |MATCH (source:${source._1.quote} {$sourceKey: row.source.$sourceKey})
        |MATCH (target:${target._1.quote} {$targetKey: row.target.$targetKey})
        |MERGE (source)-[rel:${relationship._1.quote}]->(target) ON CREATE SET rel += row.relationship
        |""".stripMargin

    execute(sc, dataFrame, partitions, unwindBatchSize, relStatement, (r: Row) => Map(
      "source" -> source._2.map(c => (renamedColumns.getOrElse(c, c), toJava(r.getAs(c)))).toMap.asJava,
      "target" -> target._2.map(c => (renamedColumns.getOrElse(c, c), toJava(r.getAs(c)))).toMap.asJava,
      "relationship" -> relationship._2.map(c => (c, toJava(r.getAs(c)))).toMap.asJava).asJava
    )
  }

  private def execute(sc: SparkContext,
                      dataFrame: DataFrame,
                      partitions: Int,
                      unwindBatchSize: Int,
                      statement: String,
                      mapFun: Row => Any) {
    val config = Neo4jConfig(sc.getConf)
    dataFrame.repartition(partitions).foreachPartition(rows => {
      val driver: Driver = config.driver()
      val session = driver.session(config.sessionConfig())
      try {
        rows.grouped(unwindBatchSize)
          .foreach(chunk => {
            val params: AnyRef = chunk.map(mapFun).asJava
            session.writeTransaction(new TransactionWork[ResultSummary]() {
              override def execute(tx: Transaction): ResultSummary =
                tx.run(statement, Map("rows" -> params).asJava).consume()
            })
          })
      } finally {
        close(driver, session)
      }
    })
  }

  def createNodes(sc: SparkContext,
                  dataFrame: DataFrame,
                  nodes: (String, Seq[String]),
                  renamedColumns: Map[String, String] = Map.empty,
                  partitions: Int = 1,
                  unwindBatchSize: Int = 10000,
                  merge: Boolean = false): Unit = {
    val nodeLabel: String = renamedColumns.getOrElse(nodes._2.head, nodes._2.head)
    val createStatement = s"""|
       |UNWIND $$rows as row
       |${if (merge) "MERGE" else "CREATE"} (node:${nodes._1.quote} {${nodeLabel.quote} : row.node_properties.${nodeLabel.quote}})
       |SET node += row.node_properties
       |""".stripMargin
    execute(sc, dataFrame, partitions, unwindBatchSize, createStatement, (r: Row) => Map(
      "node_properties" -> nodes._2.map(c => (renamedColumns.getOrElse(c, c), toJava(r.getAs(c)))).toMap.asJava).asJava
    )
  }

  def withDataType(sqlContext: SQLContext, query: String, parameters: Seq[(String, Any)], schema: (String, DataType)*) = {
    val rowRdd = Neo4jRowRDD(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, CypherTypes.schemaFromDataType(schema))
  }

  def apply(sqlContext: SQLContext, query: String, parameters: Seq[(String, Any)], schema: (String, String)*) = {
    val rowRdd = Neo4jRowRDD(sqlContext.sparkContext, query, parameters)
    sqlContext.createDataFrame(rowRdd, CypherTypes.schemaFromNamedType(schema))
  }

  def toJava(x: Any): Any = {
    import scala.collection.JavaConverters._
    x match {
      case y: scala.collection.MapLike[_, _, _] =>
        y.map { case (d, v) => toJava(d) -> toJava(v) } asJava
      case y: scala.collection.SetLike[_,_] =>
        y map { item: Any => toJava(item) } asJava
      case y: Iterable[_] =>
        y.map { item: Any => toJava(item) } asJava
      case y: Iterator[_] =>
        toJava(y.toIterable)
      case _ =>
        Neo4jUtils.convertFromSpark(x)
    }
  }

  def apply(sqlContext: SQLContext, query: String, parameters: java.util.Map[String, AnyRef]): DataFrame = {
    val limitedQuery = s"$query LIMIT 1"
    val config = Neo4jConfig(sqlContext.sparkContext.getConf)
    val driver = config.driver()
    val session = driver.session(config.sessionConfig())
    val (peekSize, schema) = try {
      val runTransaction = new TransactionWork[(Int, StructType)]() {
        override def execute(tx: Transaction): (Int, StructType) = {
          val result = tx.run(limitedQuery, parameters)
          if (!result.hasNext) throw new RuntimeException("Can't determine schema from empty result")
          val peek: Record = result.next()
          val fields = peek.keys().asScala.map(k => (k, peek.get(k).`type`())).map(keyType => CypherTypes.field(keyType))
          (peek.size, StructType(fields))
        }
      }
      session.readTransaction(runTransaction)
    } finally {
      close(driver, session)
    }
    val rowRdd = new Neo4jResultRdd(sqlContext.sparkContext, peekSize, Neo4jConfig(sqlContext.sparkContext.getConf), query, parameters)
    sqlContext.createDataFrame(rowRdd, schema)
  }

  class Neo4jResultRdd(@transient sc: SparkContext, keyCount: Int, config: Neo4jConfig, query: String, params: java.util.Map[String, AnyRef])
    extends RDD[Row](sc, Nil) {

    def convert(value: AnyRef) = value match {
      case m: java.util.Map[_, _] => m.asScala
      case _ => value
    }

    override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
      new Neo4jSessionAwareIterator(config, query, params, false).map(record => {
        val res = keyCount match {
          case 0 => Row.empty
          case 1 => Row(convert(record.get(0).asObject()))
          case _ =>
            val builder = Seq.newBuilder[AnyRef]
            builder.sizeHint(keyCount)
            var i = 0
            while (i < keyCount) {
              builder += convert(record.get(i).asObject())
              i = i + 1
            }
            Row.fromSeq(builder.result())
        }
        res
      })
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
    case "LONG" => INTEGER
    case "INT" => INTEGER
    case "INTEGER" => INTEGER
    case "FLOAT" => FlOAT
    case "DOUBLE" => FlOAT
    case "NUMERIC" => FlOAT
    case "STRING" => STRING
    case "BOOLEAN" => BOOLEAN
    case "BOOL" => BOOLEAN
    case "DATETIME" => DATETIME
    case "TIME" => DATETIME
    case "DATE" => DATE
    case "NULL" => NULL
    case _ => STRING
  }

  def apply(typ: String): DataType =
    (typ.charAt(0), typ.substring(1, typ.length - 1), typ.charAt(typ.length - 1)) match {
      case ('[', inner, ']') => DataTypes.createArrayType(typeOf(inner), true)
      case ('{', inner, '}') => DataTypes.createMapType(STRING, typeOf(inner), true)
      case _ => typeOf(typ)
    }

  //  val MAP = edges.MapType(edges.StringType,edges.AnyDataType)
  //  val LIST = edges.ArrayType(edges.AnyDataType)
  // , MAP, LIST, NODE, RELATIONSHIP, PATH


  def toSparkType(typeSystem: TypeSystem, typ: Type): org.apache.spark.sql.types.DataType =
    if (typ == typeSystem.BOOLEAN()) CypherTypes.BOOLEAN
    else if (typ == typeSystem.STRING()) CypherTypes.STRING
    else if (typ == typeSystem.INTEGER()) CypherTypes.INTEGER
    else if (typ == typeSystem.DATE()) CypherTypes.DATE
    else if (typ == typeSystem.DATE_TIME()) CypherTypes.DATETIME
    else if (typ == typeSystem.LOCAL_DATE_TIME()) CypherTypes.DATETIME
    else if (typ == typeSystem.LOCAL_TIME()) CypherTypes.DATETIME
    else if (typ == typeSystem.TIME()) CypherTypes.DATETIME
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
