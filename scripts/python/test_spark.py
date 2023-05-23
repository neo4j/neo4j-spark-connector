from pyspark.sql import SparkSession
import datetime
from testcontainers.neo4j import Neo4jContainer
from tzlocal import get_localzone

import unittest
import sys


class SparkTest(unittest.TestCase):
    neo4j_session = None
    neo4j_container = None
    spark = None

    def tearDown(self):
        self.neo4_session.run("MATCH (n) DETACH DELETE n;")

    def init_test(self, query, parameters=None):
        self.neo4_session.run(query, parameters)
        return self.spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", self.neo4j_container.get_connection_url()) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", "neo4j") \
            .option("authentication.basic.password", "password") \
            .option("labels", "Person") \
            .load()

    def test_string(self):
        name = "Foobar"
        df = self.init_test(
            "CREATE (p:Person {name: '" + name + "'})")

        assert name == df.select("name").collect()[0].name

    def test_int(self):
        age = 32
        df = self.init_test(
            "CREATE (p:Person {age: " + str(age) + "})")

        assert age == df.select("age").collect()[0].age

    def test_double(self):
        score = 32.3
        df = self.init_test(
            "CREATE (p:Person {score: " + str(score) + "})")

        assert score == df.select("score").collect()[0].score

    def test_boolean(self):
        df = self.init_test("CREATE (p:Person {boolean: true})")

        assert True == df.select("boolean").collect()[0].boolean

    def test_time(self):
        time = datetime.time(12, 23, 0, 0, get_localzone())
        df = self.init_test(
            "CREATE (p:Person {myTime: time({hour:12, minute: 23, second: 0})})"
        )

        timeResult = df.select("myTime").collect()[0].myTime

        assert "offset-time" == timeResult.type
        # .replace used in case of UTC timezone because of https://stackoverflow.com/a/42777551/1409772
        assert str(time).replace("+00:00", "Z") \
               == timeResult.value.split("+")[0]

    def test_datetime(self):
        dtString = "2015-06-24T12:50:35"
        df = self.init_test(
            "CREATE (p:Person {datetime: datetime('" + dtString + "')})")

        dt = datetime.datetime(2015, 6, 24, 12, 50, 35, 0)
        dtResult = df.select("datetime").collect()[0].datetime

        assert dt == dtResult

    def test_date(self):
        df = self.init_test(
            "CREATE (p:Person {born: date('2009-10-10')})")

        dt = datetime.date(2009, 10, 10)
        dtResult = df.select("born").collect()[0].born

        assert dt == dtResult

    def test_point(self):
        df = self.init_test(
            "CREATE (p:Person {location: point({x: 12.12, y: 13.13})})"
        )

        pointResult = df.select("location").collect()[0].location
        assert "point-2d" == pointResult[0]
        assert 7203 == pointResult[1]
        assert 12.12 == pointResult[2]
        assert 13.13 == pointResult[3]

    def test_point3d(self):
        df = self.init_test(
            "CREATE (p:Person {location: point({x: 12.12, y: 13.13, z: 1})})"
        )

        pointResult = df.select("location").collect()[0].location
        assert "point-3d" == pointResult[0]
        assert 9157 == pointResult[1]
        assert 12.12 == pointResult[2]
        assert 13.13 == pointResult[3]
        assert 1.0 == pointResult[4]

    def test_geopoint(self):
        df = self.init_test(
            "CREATE (p:Person {location: point({longitude: 12.12, latitude: 13.13})})"
        )

        pointResult = df.select("location").collect()[0].location
        assert "point-2d" == pointResult[0]
        assert 4326 == pointResult[1]
        assert 12.12 == pointResult[2]
        assert 13.13 == pointResult[3]

    def test_duration(self):
        df = self.init_test(
            "CREATE (p:Person {range: duration({days: 14, hours:16, minutes: 12})})"
        )

        durationResult = df.select("range").collect()[0].range
        assert "duration" == durationResult[0]
        assert 0 == durationResult[1]
        assert 14 == durationResult[2]
        assert 58320 == durationResult[3]
        assert 0 == durationResult[4]

    def test_string_array(self):
        df = self.init_test(
            "CREATE (p:Person {names: ['John', 'Doe']})")

        result = df.select("names").collect()[0].names
        assert "John" == result[0]
        assert "Doe" == result[1]

    def test_int_array(self):
        df = self.init_test("CREATE (p:Person {ages: [24, 56]})")

        result = df.select("ages").collect()[0].ages
        assert 24 == result[0]
        assert 56 == result[1]

    def test_double_array(self):
        df = self.init_test(
            "CREATE (p:Person {scores: [24.11, 56.11]})")

        result = df.select("scores").collect()[0].scores
        assert 24.11 == result[0]
        assert 56.11 == result[1]

    def test_boolean_array(self):
        df = self.init_test(
            "CREATE (p:Person {field: [true, false]})")

        result = df.select("field").collect()[0].field
        assert True == result[0]
        assert False == result[1]

    def test_time_array(self):
        df = self.init_test(
            "CREATE (p:Person {result: [time({hour:11, minute: 23, second: 0}), time({hour:12, minute: 23, second: 0})]})"
        )

        timeResult = df.select("result").collect()[0].result

        # .replace used in case of UTC timezone because of https://stackoverflow.com/a/42777551/1409772
        assert "offset-time" == timeResult[0].type
        assert str(datetime.time(11, 23, 0, 0, get_localzone())).replace("+00:00", "Z") \
               == timeResult[0].value.split("+")[0]

        # .replace used in case of UTC timezone because of https://stackoverflow.com/a/42777551/1409772
        assert "offset-time" == timeResult[1].type
        assert str(datetime.time(12, 23, 0, 0, get_localzone())).replace("+00:00", "Z") \
               == timeResult[1].value.split("+")[0]

    def test_datetime_array(self):
        df = self.init_test(
            "CREATE (p:Person {result: [datetime('2007-12-03T10:15:30'), datetime('2008-12-03T10:15:30')]})"
        )

        dt1 = datetime.datetime(
            2007, 12, 3, 10, 15, 30, 0)
        dt2 = datetime.datetime(
            2008, 12, 3, 10, 15, 30, 0)
        dtResult = df.select("result").collect()[0].result

        assert dt1 == dtResult[0]
        assert dt2 == dtResult[1]

    def test_date_array(self):
        df = self.init_test(
            "CREATE (p:Person {result: [date('2009-10-10'), date('2008-10-10')]})"
        )

        dt1 = datetime.date(2009, 10, 10)
        dt2 = datetime.date(2008, 10, 10)
        dtResult = df.select("result").collect()[0].result

        assert dt1 == dtResult[0]
        assert dt2 == dtResult[1]

    def test_point_array(self):
        df = self.init_test(
            "CREATE (p:Person {location: [point({x: 12.12, y: 13.13}), point({x: 13.13, y: 14.14})]})"
        )

        pointResult = df.select("location").collect()[0].location
        assert "point-2d" == pointResult[0][0]
        assert 7203 == pointResult[0][1]
        assert 12.12 == pointResult[0][2]
        assert 13.13 == pointResult[0][3]

        assert "point-2d" == pointResult[1][0]
        assert 7203 == pointResult[1][1]
        assert 13.13 == pointResult[1][2]
        assert 14.14 == pointResult[1][3]

    def test_point3d_array(self):
        df = self.init_test(
            "CREATE (p:Person {location: [point({x: 12.12, y: 13.13, z: 1}), point({x: 14.14, y: 15.15, z: 1})]})"
        )

        pointResult = df.select("location").collect()[0].location
        assert "point-3d" == pointResult[0][0]
        assert 9157 == pointResult[0][1]
        assert 12.12 == pointResult[0][2]
        assert 13.13 == pointResult[0][3]
        assert 1.0 == pointResult[0][4]

        assert "point-3d" == pointResult[1][0]
        assert 9157 == pointResult[1][1]
        assert 14.14 == pointResult[1][2]
        assert 15.15 == pointResult[1][3]
        assert 1.0 == pointResult[1][4]

    def test_geopoint_array(self):
        df = self.init_test(
            "CREATE (p:Person {location: [point({longitude: 12.12, latitude: 13.13}), point({longitude: 14.14, latitude: 15.15})]})"
        )

        pointResult = df.select("location").collect()[0].location
        assert "point-2d" == pointResult[0][0]
        assert 4326 == pointResult[0][1]
        assert 12.12 == pointResult[0][2]
        assert 13.13 == pointResult[0][3]

        assert "point-2d" == pointResult[1][0]
        assert 4326 == pointResult[1][1]
        assert 14.14 == pointResult[1][2]
        assert 15.15 == pointResult[1][3]

    def test_duration_array(self):
        df = self.init_test(
            "CREATE (p:Person {range: [duration({days: 14, hours:16, minutes: 12}), duration({days: 15, hours:16, minutes: 12})]})"
        )

        durationResult = df.select("range").collect()[0].range
        assert "duration" == durationResult[0][0]
        assert 0 == durationResult[0][1]
        assert 14 == durationResult[0][2]
        assert 58320 == durationResult[0][3]
        assert 0 == durationResult[0][4]

        assert "duration" == durationResult[1][0]
        assert 0 == durationResult[1][1]
        assert 15 == durationResult[1][2]
        assert 58320 == durationResult[1][3]
        assert 0 == durationResult[1][4]

    def test_unexisting_property(self):
        self.spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", self.neo4j_container.get_connection_url()) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", "neo4j") \
            .option("authentication.basic.password", "password") \
            .option("relationship.properties", None) \
            .option("relationship", "FOO") \
            .option("relationship.source.labels", ":Foo") \
            .option("relationship.target.labels", ":Bar") \
            .load()
        # In this case we just test that the job has been executed without any exception

    def test_gds(self):
        self.neo4_session.run("""
            CREATE
              (home:Page {name:'Home'}),
              (about:Page {name:'About'}),
              (product:Page {name:'Product'}),
              (links:Page {name:'Links'}),
              (a:Page {name:'Site A'}),
              (b:Page {name:'Site B'}),
              (c:Page {name:'Site C'}),
              (d:Page {name:'Site D'}),
            
              (home)-[:LINKS {weight: 0.2}]->(about),
              (home)-[:LINKS {weight: 0.2}]->(links),
              (home)-[:LINKS {weight: 0.6}]->(product),
              (about)-[:LINKS {weight: 1.0}]->(home),
              (product)-[:LINKS {weight: 1.0}]->(home),
              (a)-[:LINKS {weight: 1.0}]->(home),
              (b)-[:LINKS {weight: 1.0}]->(home),
              (c)-[:LINKS {weight: 1.0}]->(home),
              (d)-[:LINKS {weight: 1.0}]->(home),
              (links)-[:LINKS {weight: 0.8}]->(home),
              (links)-[:LINKS {weight: 0.05}]->(a),
              (links)-[:LINKS {weight: 0.05}]->(b),
              (links)-[:LINKS {weight: 0.05}]->(c),
              (links)-[:LINKS {weight: 0.05}]->(d);
        """)

        self.spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", self.neo4j_container.get_connection_url()) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", "neo4j") \
            .option("authentication.basic.password", "password") \
            .option("gds", "gds.graph.project") \
            .option("gds.graphName", "myGraph") \
            .option("gds.nodeProjection", "Page") \
            .option("gds.relationshipProjection", "LINKS") \
            .option("gds.configuration.relationshipProperties", "weight") \
            .load() \
            .show(truncate=False)

        df = self.spark.read.format("org.neo4j.spark.DataSource") \
            .option("url", self.neo4j_container.get_connection_url()) \
            .option("authentication.type", "basic") \
            .option("authentication.basic.username", "neo4j") \
            .option("authentication.basic.password", "password") \
            .option("gds", "gds.pageRank.stream") \
            .option("gds.graphName", "myGraph") \
            .option("gds.configuration.concurrency", "2") \
            .load()

        assert 8 == df.count()


if len(sys.argv) != 5:
    print("Wrong arguments count")
    print(sys.argv)
    sys.exit(1)

connector_version = str(sys.argv.pop())
neo4j_version = str(sys.argv.pop())
scala_version = str(sys.argv.pop())
spark_version = str(sys.argv.pop())
current_time_zone = get_localzone().zone

print("Running tests for Connector %s, Neo4j %s, Scala %s, Spark %s, TimeZone %s"
      % (connector_version, neo4j_version, scala_version, spark_version, current_time_zone))

if __name__ == "__main__":
    with (Neo4jContainer('neo4j:' + neo4j_version)
            .with_env("NEO4J_db_temporal_timezone", current_time_zone)
            .with_env("NEO4JLABS_PLUGINS", "[\"graph-data-science\"]")) as neo4j_container:
        with neo4j_container.get_driver() as neo4j_driver:
            with neo4j_driver.session() as neo4j_session:
                SparkTest.spark = SparkSession.builder \
                    .appName("Neo4jConnectorTests") \
                    .master('local[*]') \
                    .config(
                    "spark.jars",
                    "../../spark-%s/target/neo4j-connector-apache-spark_%s-%s_for_spark_%s.jar"
                    % (spark_version, scala_version, connector_version, spark_version)
                ) \
                    .config("spark.driver.host", "127.0.0.1") \
                    .getOrCreate()
                SparkTest.neo4_session = neo4j_session
                SparkTest.neo4j_container = neo4j_container
                unittest.main()
                SparkTest.spark.close()
