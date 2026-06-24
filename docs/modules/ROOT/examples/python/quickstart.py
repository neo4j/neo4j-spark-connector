from pyspark.sql import SparkSession

# Replace with the actual connection URI and credentials
url = "neo4j://localhost:7687"
username = "neo4j"
password = "password"
dbname = "neo4j"

spark = (
    SparkSession.builder.config("neo4j.url", url)
    .config("neo4j.authentication.basic.username", username)
    .config("neo4j.authentication.basic.password", password)
    .config("neo4j.database", dbname)
    .getOrCreate()
)

# Create example DataFrame
df = spark.createDataFrame(
    [
        {"name": "John", "surname": "Doe", "age": 42},
        {"name": "Jane", "surname": "Doe", "age": 40},
    ]
)

# Write to Neo4j
(
    df.write.format("org.neo4j.spark.DataSource")
    .mode("Overwrite")
    .option("labels", "Person")
    .option("node.keys", "name,surname")
    .save()
)

# Read from Neo4j
(
    spark.read.format("org.neo4j.spark.DataSource")
    .option("labels", "Person")
    .load()
    .show()
)
