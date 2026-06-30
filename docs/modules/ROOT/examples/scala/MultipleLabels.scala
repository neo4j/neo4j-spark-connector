// tag::setup[]
import org.apache.spark.sql.{SaveMode, SparkSession}

// Replace with the actual connection URI and credentials
val url = "neo4j://localhost:7687"
val username = "neo4j"
val password = "password"
val dbname = "neo4j"

val spark = SparkSession.builder
    .config("neo4j.url", url)
    .config("neo4j.authentication.basic.username", username)
    .config("neo4j.authentication.basic.password", password)
    .config("neo4j.database", dbname)
    .getOrCreate()
// end::setup[]

case class Person(name: String, surname: String, age: Int)

val peopleDF = List(
    Person("John", "Doe", 42),
    Person("Jane", "Doe", 40)
).toDF()

// tag::code-write[]
peopleDF.write
    .format("org.neo4j.spark.DataSource")
    .mode(SaveMode.Append)
    // ":Person:Employee" and "Person:Employee"
    // are equivalent
    .option("labels", ":Person:Employee")
    .save()
// end::code-write[]

// tag::code-read[]
val df = spark.read
    .format("org.neo4j.spark.DataSource")
    // ":Person:Employee" and "Person:Employee"
    // are equivalent
    .option("labels", ":Person:Employee")
    .load()

df.show()
// end::code-read[]

// tag::check[]
// TODO: add read query to check
// end::check[]
