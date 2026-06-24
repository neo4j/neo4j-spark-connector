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

# tag::code-write[]
# Create example DataFrame
queryDF = spark.createDataFrame(
    [
        {"name": "John", "surname": "Doe", "age": 42},
        {"name": "Jane", "surname": "Doe", "age": 40},
    ]
)

# Define the Cypher query to use in the write
write_query = "CREATE (n:Person {fullName: event.name + ' ' + event.surname})"

(
    queryDF.write.format("org.neo4j.spark.DataSource")
    .option("query", write_query)
    .mode("Overwrite")
    .save()
)
# end::code-write[]

# tag::code-read[]
read_query = """
    MATCH (n:Person)
    RETURN id(n) AS id, n.fullName AS name
"""

df = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("query", read_query)
    .load()
)

df.show()
# end::code-read[]

# tag::check[]
# TODO: add read query to check
# end::check[]
