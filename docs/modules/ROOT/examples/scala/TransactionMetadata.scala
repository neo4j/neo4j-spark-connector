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

// tag::code-single-metadata[]
val df = spark.read
    .format("org.neo4j.spark.DataSource")
    .option("query", "RETURN 42 AS example")
    .option("db.transaction.metadata.fromMySparkApp", true)
    .option("db.transaction.metadata.anotherMetadata", "free text goes here")
    .load()
// end::code-single-metadata[]

// tag::code-nested-metadata[]
val df = spark.read
    .format("org.neo4j.spark.DataSource")
    .option("query", "RETURN 42 AS example")
    .option("db.transaction.metadata.fromMySparkApp", true)
    .option("db.transaction.metadata.example.id", 55)
    .option("db.transaction.metadata.example.message", "another message")
    .load()
// end::code-nested-metadata[]
