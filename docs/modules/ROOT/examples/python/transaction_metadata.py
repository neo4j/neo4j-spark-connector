# tag::setup[]
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
# end::setup[]

# tag::code-single-metadata[]
df = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("query", "RETURN 42 AS example")
    .option("db.transaction.metadata.fromMySparkApp", True)
    .option("db.transaction.metadata.withMessage", "free text goes here")
    .load()
)
# end::code-single-metadata[]
#
# tag::code-nested-metadata[]
df = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("query", "RETURN 42 AS example")
    .option("db.transaction.metadata.fromMySparkApp", True)
    .option("db.transaction.metadata.example.id", 55)
    .option("db.transaction.metadata.example.message", "another message")
    .load()
)
# end::code-nested-metadata[]
