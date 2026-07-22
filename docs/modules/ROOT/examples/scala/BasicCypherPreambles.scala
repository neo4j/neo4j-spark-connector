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

// tag::code-runtime-parallel[]
val df = spark.read
    .format("org.neo4j.spark.DataSource")
    .option("query", "MATCH (o: Object) RETURN o")
    .option("cypher.tuning.runtime", "parallel")
    .load()
// end::code-runtime-parallel[]

// tag::code-multiple-tuning[]
val df = spark.read
    .format("org.neo4j.spark.DataSource")
    .option("query", "MATCH (o: Object) RETURN o")
    .option("cypher.tuning.runtime", "parallel")
    .option("cypher.tuning.operatorEngine", "interpreted")
    .load()
// end::code-multiple-tuning[]

// tag::code-cypher-version[]
val df = spark.read
    .format("org.neo4j.spark.DataSource")
    .option("query", "MATCH (o: Object) RETURN o")
    .option("cypher.version", "25")
    .load()
// end::code-cypher-version[]
