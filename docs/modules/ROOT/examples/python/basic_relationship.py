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
relDF = spark.createDataFrame(
    [
        {
            "name": "John",
            "surname": "Doe",
            "customerID": 1,
            "product": "Product 1",
            "quantity": 200,
            "order": "ABC100",
        },
        {
            "name": "Jane",
            "surname": "Doe",
            "customerID": 2,
            "product": "Product 2",
            "quantity": 100,
            "order": "ABC200",
        },
    ]
)

(
    relDF.write
    # Create new relationships
    .mode("Append")
    .format("org.neo4j.spark.DataSource")
    # Assign a type to the relationships
    .option("relationship", "BOUGHT")
    # Use `keys` strategy
    .option("relationship.save.strategy", "keys")
    # Create source nodes and assign them a label
    .option("relationship.source.save.mode", "Append")
    .option("relationship.source.labels", ":Customer")
    # Map the DataFrame columns to node properties
    .option("relationship.source.node.properties", "name,surname,customerID:id")
    # Create target nodes and assign them a label
    .option("relationship.target.save.mode", "Append")
    .option("relationship.target.labels", ":Product")
    # Map the DataFrame columns to node properties
    .option("relationship.target.node.properties", "product:name")
    # Map the DataFrame columns to relationship properties
    .option("relationship.properties", "quantity,order")
    .save()
)
# end::code-write[]

# tag::code-read[]
df = (
    spark.read.format("org.neo4j.spark.DataSource")
    .option("relationship", "BOUGHT")
    .option("relationship.source.labels", ":Customer")
    .option("relationship.target.labels", ":Product")
    .load()
)

df.show()
# end::code-read[]

# tag::check[]
# TODO: add read query to check
# end::check[]
