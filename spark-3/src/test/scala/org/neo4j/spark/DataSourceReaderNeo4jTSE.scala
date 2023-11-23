package org.neo4j.spark

import org.apache.spark.SparkException
import org.apache.spark.sql.DataFrame
import org.junit.Assert.{assertEquals, assertTrue, fail}
import org.junit.{Assume, BeforeClass, Test}
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.{SessionConfig, Transaction, TransactionWork}
import org.neo4j.driver.summary.ResultSummary

class DataSourceReaderNeo4jTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def testMultiDbJoin(): Unit = {
    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db1"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
      CREATE (p1:Person:Customer {name: 'John Doe'}),
       (p2:Person:Customer {name: 'Mark Brown'}),
       (p3:Person:Customer {name: 'Cindy White'})
      """).consume()
        })

    SparkConnectorScalaSuiteIT.driver.session(SessionConfig.forDatabase("db2"))
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(
            """
      CREATE (p1:Person:Employee {name: 'Jane Doe'}),
       (p2:Person:Employee {name: 'John Doe'})
      """).consume()
        })

    val df1 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db1")
      .option("labels", "Person")
      .load()

    val df2 = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("database", "db2")
      .option("labels", "Person")
      .load()

    assertEquals(3, df1.count())
    assertEquals(2, df2.count())

    val dfJoin = df1.join(df2, df1("name") === df2("name"))
    assertEquals(1, dfJoin.count())
  }

  @Test
  def testReadQueryCustomPartitions(): Unit = {
    val fixtureProduct1Query: String =
      """CREATE (pr:Product{id: 1, name: 'Product 1'})
        |WITH pr
        |UNWIND range(1,100) as id
        |CREATE (p:Person {id: id, name: 'Person ' + id})-[:BOUGHT{quantity: ceil(rand() * 100)}]->(pr)
        |RETURN *
    """.stripMargin
    SparkConnectorScalaSuiteIT.driver.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureProduct1Query).consume()
        })
    val fixtureProduct2Query: String =
      """CREATE (pr:Product{id: 2, name: 'Product 2'})
        |WITH pr
        |UNWIND range(1,50) as id
        |MATCH (p:Person {id: id})
        |CREATE (p)-[:BOUGHT{quantity: ceil(rand() * 100)}]->(pr)
        |RETURN *
    """.stripMargin
    SparkConnectorScalaSuiteIT.driver.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureProduct2Query).consume()
        })

    val partitionedDf = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query",
        """
          |MATCH (p:Person)-[r:BOUGHT]->(pr:Product)
          |RETURN p.name AS person, pr.name AS product, r.quantity AS quantity""".stripMargin)
      .option("partitions", "5")
      .load()

    assertEquals(5, partitionedDf.rdd.getNumPartitions)
    val rows = partitionedDf.collect()
      .map(row => s"${row.getAs[String]("person")}-${row.getAs[String]("product")}")
    assertEquals(150, rows.size)
    assertEquals(150, rows.size)
  }

  @Test
  def testCallShouldReturnCorrectSchema(): Unit = {
    val callDf: DataFrame = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query", "CALL db.info() YIELD id, name RETURN *")
      .load()

    val res = callDf.select("name")
      .collectAsList()
      .get(0)

    assertEquals(res.getString(0), "neo4j")
  }

  @Test
  def testShouldReturnJustTheSelectedFieldWithNode(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id, name: 'Product ' + id})
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df = ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Product")
      .load
      .select("name")

    df.count()

    assertEquals(Seq("name"), df.columns.toSeq)
  }

  @Test
  def testShouldReturnJustTheSelectedFieldWithNodeAndWeirdColumnName(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id, `(╯°□°)╯︵ ┻━┻`: 'Product ' + id})
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df = ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Product")
      .load
      .select("`(╯°□°)╯︵ ┻━┻`")

    df.count()

    assertEquals(Seq("(╯°□°)╯︵ ┻━┻"), df.columns.toSeq)
  }

  @Test
  def testShouldReturnJustTheSelectedFieldWithRelationship(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df = ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "BOUGHT")
      .option("relationship.source.labels", "Product")
      .option("relationship.target.labels", "Person")
      .load
      .select("`source.name`", "`<source.id>`")

    df.count()

    assertEquals(Seq("source.name", "<source.id>"), df.columns.toSeq)
  }

  @Test
  def testShouldReturnJustTheSelectedFieldWithRelationshipAndWeirdColumn(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * rand(), `(╯°□°)╯︵ ┻━┻`: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df = ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "BOUGHT")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Product")
      .load
      .select("`target.(╯°□°)╯︵ ┻━┻`", "`<source.id>`")

    df.count()

    assertEquals(Seq("target.(╯°□°)╯︵ ┻━┻", "<source.id>"), df.columns.toSeq)
  }

  @Test
  def testShouldReturnJustTheSelectedFieldWithQuery(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df = ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query", "MATCH (p:Product) RETURN p.name as name")
      .option("partitions", 2)
      .option("query.count", 20)
      .load
      .select("name")

    df.count()

    assertEquals(Seq("name"), df.columns.toSeq)
  }

  @Test
  def testShouldReturnJustTheSelectedFieldWithFilter(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id, name: 'Product ' + id})
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df = ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", "Product")
      .load
      .filter("name = 'Product 1'")

    df.count()

    assertEquals(Seq("<id>", "<labels>", "name", "id"), df.columns.toSeq)
  }

  @Test
  def testShouldReturnJustTheSelectedFieldWithRelationshipWithFilter(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df = ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "BOUGHT")
      .option("relationship.source.labels", "Person")
      .option("relationship.target.labels", "Product")
      .load
      .filter("`target.name` = 'Product 1' AND `target.id` = '16'")
      .select("`target.name`", "`target.id`")

    df.count()

    assertEquals(Seq("target.name", "target.id"), df.columns.toSeq)
  }

  @Test
  def testShouldThrowClearErrorIfAWrongDbIsSpecified(): Unit = {
    try {
      ss.read.format(classOf[DataSource].getName)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("database", "not_existing_db")
        .option("labels", "MATCH (h:Household) RETURN id(h)")
        .load()
        .show()
    }
    catch {
      case clientException: ClientException => {
        assertTrue(clientException.getMessage.equals(
          "Database does not exist. Database name: 'not_existing_db'."
        ))
      }
      case generic: Throwable => fail(s"should be thrown a ${classOf[SparkException].getName}, got ${generic.getClass} instead")
    }
  }

  @Test
  def testEmptyDataset(): Unit = {
    val df = ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query", "MATCH (e:ID_DO_NOT_EXIST) RETURN id(e) as f, 1 as g")
      .load

    assertEquals(0, df.count())
    assertEquals(Seq("f", "g"), df.columns.toSeq)
  }

  @Test
  def testColumnSorted(): Unit = {
    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run("CREATE (i1:Instrument{name: 'Drums', id: 1}), (i2:Instrument{name: 'Guitar', id: 2})").consume()
        })

    val df = ss.read
      .format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query", "MATCH (i:Instrument) RETURN id(i) as internal_id, i.id as id, i.name as name, i.name")
      .load
      .orderBy("id")

    assertEquals(1L, df.collectAsList().get(0).get(1))
    assertEquals("Drums", df.collectAsList().get(0).get(2))
    assertEquals(Seq("internal_id", "id", "name", "i.name"), df.columns.toSeq)
  }

  @Test
  def testComplexReturnStatement(): Unit = {
    val total = 100
    val fixtureQuery: String =
      s"""UNWIND range(1, $total) as id
         |CREATE (pr:Product {id: id * rand(), name: 'Product ' + id})
         |CREATE (pe:Person {id: id, fullName: 'Person ' + id})
         |CREATE (pe)-[:BOUGHT{when: rand(), quantity: rand() * 1000}]->(pr)
         |RETURN *
    """.stripMargin

    SparkConnectorScalaSuiteIT.session()
      .writeTransaction(
        new TransactionWork[ResultSummary] {
          override def execute(tx: Transaction): ResultSummary = tx.run(fixtureQuery).consume()
        })

    val df = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query",
        """MATCH (p:Person)-[b:BOUGHT]->(pr:Product)
          |RETURN id(p) AS personId, id(pr) AS productId, {quantity: b.quantity, when: b.when} AS map, "some string" as someString, {anotherField: "201"} as map2""".stripMargin)
      .option("schema.strategy", "string")
      .load()

    assertEquals(Seq("personId", "productId", "map", "someString", "map2"), df.columns.toSeq)
    assertEquals(100, df.count())
  }

  @Test
  def testComplexReturnStatementNoValues(): Unit = {
    val df = ss.read.format(classOf[DataSource].getName)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("query",
        """MATCH (p:Person)-[b:BOUGHT]->(pr:Product)
          |RETURN id(p) AS personId, id(pr) AS productId, {quantity: b.quantity, when: b.when} AS map, "some string" as someString, {anotherField: "201", and: 1} as map2""".stripMargin)
      .option("schema.strategy", "string")
      .load()

    assertEquals(Seq("personId", "productId", "map", "someString", "map2"), df.columns.toSeq)
    assertEquals(0, df.count())
  }
}
