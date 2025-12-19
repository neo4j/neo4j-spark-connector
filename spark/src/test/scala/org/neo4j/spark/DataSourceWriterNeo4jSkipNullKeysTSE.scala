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
package org.neo4j.spark

import org.apache.spark.SparkException
import org.apache.spark.sql.SaveMode
import org.junit.Assume
import org.junit.Test
import org.neo4j.Closeables.use
import org.neo4j.caniuse.CanIUse
import org.neo4j.caniuse.Schema

class DataSourceWriterNeo4jSkipNullKeysTSE extends SparkConnectorScalaBaseTSE {

  import ss.implicits._

  @Test
  def `fails to write nodes when key properties contain null values`(): Unit = {
    val cities = Seq(
      (Some(1), "Cherbourg en Cotentin"),
      (Some(2), "London"),
      (Some(3), "Malmö"),
      (None, "Moon")
    ).toDF("id", "city")

    val caught = intercept[SparkException] {
      cities.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("labels", ":City")
        .option("node.keys", "id")
        .option("schema.optimization.node.keys", "KEY")
        .save()
    }
    assert(caught.getMessage contains "Cannot merge the following node because of null property value")
  }

  @Test
  def `fails to write relationships when source node key properties contain null values`(): Unit = {
    val caught = intercept[SparkException] {
      val cities = Seq(
        (Some(1), Some(2), "British Airways"),
        (Some(2), Some(3), "Turkish Airlines"),
        (None, Some(5), "Another Airline")
      ).toDF("from", "to", "airline")

      cities.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("relationship", "FLIES_TO")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":City")
        .option("relationship.source.node.keys", "from:id")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":City")
        .option("relationship.target.node.keys", "to:id")
        .option("relationship.properties", "airline")
        .option("schema.optimization.node.keys", "KEY")
        .save()
    }
    assert(caught.getMessage contains "Cannot merge the following node because of null property value")
  }

  @Test
  def `fails to write relationships when target node key properties contain null values`(): Unit = {
    val caught = intercept[SparkException] {
      val cities = Seq(
        (Some(1), Some(2), "British Airways"),
        (Some(2), Some(3), "Turkish Airlines"),
        (Some(3), None, "Another Airline")
      ).toDF("from", "to", "airline")

      cities.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("relationship", "FLIES_TO")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":City")
        .option("relationship.source.node.keys", "from:id")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":City")
        .option("relationship.target.node.keys", "to:id")
        .option("relationship.properties", "airline")
        .option("schema.optimization.node.keys", "KEY")
        .save()
    }
    assert(caught.getMessage contains "Cannot merge the following node because of null property value")
  }

  @Test
  def `fails to write relationships when relationship key properties contain null values`(): Unit = {
    Assume.assumeTrue(
      CanIUse.INSTANCE.canIUse(Schema.INSTANCE.relationshipKeyConstraints()).withNeo4j(SparkConnectorScalaSuiteIT.neo4j)
    )

    val caught = intercept[SparkException] {
      val cities = Seq(
        (Some(1), Some(2), Some("BA721"), "British Airways"),
        (Some(2), Some(3), Some("TK211"), "Turkish Airlines"),
        (Some(3), Some(4), None, "Another Airline")
      ).toDF("from", "to", "flight", "airline")

      cities.write
        .format(classOf[DataSource].getName)
        .mode(SaveMode.Overwrite)
        .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
        .option("relationship", "FLIES_TO")
        .option("relationship.save.strategy", "keys")
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":City")
        .option("relationship.source.node.keys", "from:id")
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":City")
        .option("relationship.target.node.keys", "to:id")
        .option("relationship.keys", "flight")
        .option("relationship.properties", "airline")
        .option("schema.optimization.node.keys", "KEY")
        .option("schema.optimization.relationship.keys", "KEY")
        .save()
    }
    assert(caught.getMessage contains "Cannot merge the following relationship because of null property value")
  }

  @Test
  def `skips nodes when key properties contain null values with APPEND mode`(): Unit = {
    val cities = Seq(
      (Some(1), "Cherbourg en Cotentin"),
      (Some(2), "London"),
      (Some(3), "Malmö"),
      (None, "Moon")
    ).toDF("id", "city")

    cities.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":City")
      .option("node.keys", "id")
      .option("node.keys.skip.nulls", "true")
      .save()

    use(SparkConnectorScalaSuiteIT.driver.session()) { session =>
      val result = session.run("MATCH (n:City) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(result == 3)
    }
  }

  @Test
  def `skips nodes when key properties contain null values with OVERWRITE mode`(): Unit = {
    val cities = Seq(
      (Some(1), "Cherbourg en Cotentin"),
      (Some(2), "London"),
      (Some(3), "Malmö"),
      (None, "Moon")
    ).toDF("id", "city")

    cities.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("labels", ":City")
      .option("node.keys", "id")
      .option("node.keys.skip.nulls", "true")
      .option("schema.optimization.node.keys", "KEY")
      .save()

    use(SparkConnectorScalaSuiteIT.driver.session()) { session =>
      val result = session.run("MATCH (n:City) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(result == 3)
    }
  }

  @Test
  def `skips relationships when source or target node key properties contain null values`(): Unit = {
    val cities = Seq(
      (Some(1), Some(2), "British Airways"),
      (Some(2), Some(3), "Turkish Airlines"),
      (None, Some(5), "Another Airline"),
      (Some(5), None, "Another Airline")
    ).toDF("from", "to", "airline")

    cities.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "FLIES_TO")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.source.labels", ":City")
      .option("relationship.source.node.keys", "from:id")
      .option("relationship.target.save.mode", "Overwrite")
      .option("relationship.target.labels", ":City")
      .option("relationship.target.node.keys", "to:id")
      .option("relationship.properties", "airline")
      .option("schema.optimization.node.keys", "KEY")
      .option("relationship.source.node.keys.skip.nulls", "true")
      .option("relationship.target.node.keys.skip.nulls", "true")
      .save()

    use(SparkConnectorScalaSuiteIT.driver.session()) { session =>
      val cities = session.run("MATCH (n:City) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(cities == 3)

      val citiesWithId5 = session.run("MATCH (n:City {id: 5}) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(citiesWithId5 == 0)

      val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
        .single()
        .get("count")
        .asLong()
      assert(flies == 2)
    }
  }

  @Test
  def `skips relationships when source or target node key properties contain null values when nodes are matched`()
    : Unit = {
    use(SparkConnectorScalaSuiteIT.driver.session()) { session =>
      session.run("UNWIND [1,2,3,5] AS id CREATE (:City {id: id})").consume()
    }

    val cities = Seq(
      (Some(1), Some(2), "British Airways"),
      (Some(2), Some(3), "Turkish Airlines"),
      (None, Some(5), "Another Airline"),
      (Some(5), None, "Another Airline")
    ).toDF("from", "to", "airline")

    cities.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "FLIES_TO")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "Match")
      .option("relationship.source.labels", ":City")
      .option("relationship.source.node.keys", "from:id")
      .option("relationship.target.save.mode", "Match")
      .option("relationship.target.labels", ":City")
      .option("relationship.target.node.keys", "to:id")
      .option("relationship.properties", "airline")
      .option("relationship.source.node.keys.skip.nulls", "true")
      .option("relationship.target.node.keys.skip.nulls", "true")
      .save()

    use(SparkConnectorScalaSuiteIT.driver.session()) { session =>
      val cities = session.run("MATCH (n:City) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(cities == 4)

      val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
        .single()
        .get("count")
        .asLong()
      assert(flies == 2)
    }
  }

  @Test
  def `skips relationships when source or target node key properties contain null values when nodes are appended`()
    : Unit = {
    val cities = Seq(
      (Some(1), Some(2), "British Airways"),
      (Some(3), Some(4), "Turkish Airlines"),
      (None, Some(5), "Another Airline"),
      (Some(5), None, "Another Airline")
    ).toDF("from", "to", "airline")

    cities.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "FLIES_TO")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "Append")
      .option("relationship.source.labels", ":City")
      .option("relationship.source.node.keys", "from:id")
      .option("relationship.target.save.mode", "Append")
      .option("relationship.target.labels", ":City")
      .option("relationship.target.node.keys", "to:id")
      .option("relationship.properties", "airline")
      .option("relationship.source.node.keys.skip.nulls", "true")
      .option("relationship.target.node.keys.skip.nulls", "true")
      .save()

    use(SparkConnectorScalaSuiteIT.driver.session()) { session =>
      val cities = session.run("MATCH (n:City) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(cities == 4)

      val citiesWithId5 = session.run("MATCH (n:City {id: 5}) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(citiesWithId5 == 0)

      val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
        .single()
        .get("count")
        .asLong()
      assert(flies == 2)
    }
  }

  @Test
  def `skips relationships when source or target node key properties contain null values with append mode`(): Unit = {
    val cities = Seq(
      (Some(1), Some(2), "British Airways"),
      (Some(3), Some(4), "Turkish Airlines"),
      (None, Some(5), "Another Airline"),
      (Some(5), None, "Another Airline")
    ).toDF("from", "to", "airline")

    cities.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "FLIES_TO")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.source.labels", ":City")
      .option("relationship.source.node.keys", "from:id")
      .option("relationship.target.save.mode", "Overwrite")
      .option("relationship.target.labels", ":City")
      .option("relationship.target.node.keys", "to:id")
      .option("relationship.properties", "airline")
      .option("schema.optimization.node.keys", "KEY")
      .option("relationship.source.node.keys.skip.nulls", "true")
      .option("relationship.target.node.keys.skip.nulls", "true")
      .save()

    use(SparkConnectorScalaSuiteIT.driver.session()) { session =>
      val cities = session.run("MATCH (n:City) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(cities == 4)

      val citiesWithId5 = session.run("MATCH (n:City {id: 5}) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(citiesWithId5 == 0)

      val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
        .single()
        .get("count")
        .asLong()
      assert(flies == 2)
    }
  }

  @Test
  def `skips relationships when source or target node key properties contain null values when nodes are matched with append mode`()
    : Unit = {
    use(SparkConnectorScalaSuiteIT.driver.session()) { session =>
      session.run("UNWIND [1,2,3,5] AS id CREATE (:City {id: id})").consume()
    }

    val cities = Seq(
      (Some(1), Some(2), "British Airways"),
      (Some(2), Some(3), "Turkish Airlines"),
      (None, Some(5), "Another Airline"),
      (Some(5), None, "Another Airline")
    ).toDF("from", "to", "airline")

    cities.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "FLIES_TO")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "Match")
      .option("relationship.source.labels", ":City")
      .option("relationship.source.node.keys", "from:id")
      .option("relationship.target.save.mode", "Match")
      .option("relationship.target.labels", ":City")
      .option("relationship.target.node.keys", "to:id")
      .option("relationship.properties", "airline")
      .option("relationship.source.node.keys.skip.nulls", "true")
      .option("relationship.target.node.keys.skip.nulls", "true")
      .save()

    use(SparkConnectorScalaSuiteIT.driver.session()) { session =>
      val cities = session.run("MATCH (n:City) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(cities == 4)

      val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
        .single()
        .get("count")
        .asLong()
      assert(flies == 2)
    }
  }

  @Test
  def `skips relationships when source or target node key properties contain null values when nodes are appended with append mode`()
    : Unit = {
    val cities = Seq(
      (Some(1), Some(2), "British Airways"),
      (Some(3), Some(4), "Turkish Airlines"),
      (None, Some(5), "Another Airline"),
      (Some(5), None, "Another Airline")
    ).toDF("from", "to", "airline")

    cities.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Append)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "FLIES_TO")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "Append")
      .option("relationship.source.labels", ":City")
      .option("relationship.source.node.keys", "from:id")
      .option("relationship.target.save.mode", "Append")
      .option("relationship.target.labels", ":City")
      .option("relationship.target.node.keys", "to:id")
      .option("relationship.properties", "airline")
      .option("relationship.source.node.keys.skip.nulls", "true")
      .option("relationship.target.node.keys.skip.nulls", "true")
      .save()

    use(SparkConnectorScalaSuiteIT.driver.session()) { session =>
      val cities = session.run("MATCH (n:City) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(cities == 4)

      val citiesWithId5 = session.run("MATCH (n:City {id: 5}) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(citiesWithId5 == 0)

      val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
        .single()
        .get("count")
        .asLong()
      assert(flies == 2)
    }
  }

  @Test
  def `skips relationships when relationship key properties contain null values`(): Unit = {
    Assume.assumeTrue(
      CanIUse.INSTANCE.canIUse(Schema.INSTANCE.relationshipKeyConstraints()).withNeo4j(SparkConnectorScalaSuiteIT.neo4j)
    )

    val cities = Seq(
      (Some(1), Some(2), Some("BA721"), "British Airways"),
      (Some(2), Some(3), Some("TK211"), "Turkish Airlines"),
      (Some(3), Some(5), None, "Another Airline")
    ).toDF("from", "to", "flight", "airline")

    cities.write
      .format(classOf[DataSource].getName)
      .mode(SaveMode.Overwrite)
      .option("url", SparkConnectorScalaSuiteIT.server.getBoltUrl)
      .option("relationship", "FLIES_TO")
      .option("relationship.save.strategy", "keys")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.source.labels", ":City")
      .option("relationship.source.node.keys", "from:id")
      .option("relationship.target.save.mode", "Overwrite")
      .option("relationship.target.labels", ":City")
      .option("relationship.target.node.keys", "to:id")
      .option("relationship.keys", "flight")
      .option("relationship.properties", "airline")
      .option("schema.optimization.node.keys", "KEY")
      .option("schema.optimization.relationship.keys", "KEY")
      .option("relationship.keys.skip.nulls", "true")
      .save()

    use(SparkConnectorScalaSuiteIT.driver.session()) { session =>
      val cities = session.run("MATCH (n:City) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(cities == 3)

      val citiesWithId5 = session.run("MATCH (n:City {id: 5}) RETURN count(n) as count")
        .single()
        .get("count")
        .asLong()
      assert(citiesWithId5 == 0)

      val flies = session.run("MATCH ()-[r:FLIES_TO]->() RETURN count(r) as count")
        .single()
        .get("count")
        .asLong()
      assert(flies == 2)
    }
  }

}
