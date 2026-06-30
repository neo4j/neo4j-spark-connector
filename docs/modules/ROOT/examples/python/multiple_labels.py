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

# Create example DataFrame
peopleDF = spark.createDataFrame(
    [
        {"name": "John", "surname": "Doe", "age": 42},
        {"name": "Jane", "surname": "Doe", "age": 40},
    ]
)

# tag::code-write[]
(
    peopleDF.write.format("org.neo4j.spark.DataSource")
    .mode("Append")
    .option("labels", ":Person")
    # ":Person:Employee" and "Person:Employee"
    # are equivalent
    .option("labels", ":Person:Employee")
    .save()
)
# end::code-write[]

# tag::code-read[]
df = (
    spark.read.format("org.neo4j.spark.DataSource")
    # ":Person:Employee" and "Person:Employee"
    # are equivalent
    .option("labels", ":Person:Employee").load()
)

df.show()
# end::code-read[]

# tag::check[]
# TODO: add read query to check
# end::check[]
