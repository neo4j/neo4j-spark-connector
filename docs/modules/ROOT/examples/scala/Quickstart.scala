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

// Create example DataFrame
val df = List(
    ("John", "Doe", 42),
    ("Jane", "Doe", 40)
).toDF("name", "surname", "age")

// Write to Neo4j
df.write
    .format("org.neo4j.spark.DataSource")
    .mode(SaveMode.Overwrite)
    .option("labels", "Person")
    .option("node.keys", "name,surname")
    .save()

// Read from Neo4j
spark.read
    .format("org.neo4j.spark.DataSource")
    .option("labels", "Person")
    .load()
    .show()
