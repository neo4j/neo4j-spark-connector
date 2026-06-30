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

def nativeStrategy() = {
    // tag::code-native[]
    // Columns representing node/relationships properties
    // must use the "rel.", "source.", or "target." prefix
    val relDF = Seq(
        ("John", "Doe", 1, "Product 1", 200, "ABC100"),
        ("Jane", "Doe", 2, "Product 2", 100, "ABC200")
    ).toDF(
        "source.name",
        "source.surname",
        "source.id",
        "target.name",
        "rel.quantity",
        "rel.order"
    )

    relDF.write
        // Create new relationships
        .mode(SaveMode.Append)
        .format("org.neo4j.spark.DataSource")
        // Assign a type to the relationships
        .option("relationship", "BOUGHT")
        // Create source nodes and assign them a label
        .option("relationship.source.save.mode", "Append")
        .option("relationship.source.labels", ":Customer")
        // Create target nodes and assign them a label
        .option("relationship.target.save.mode", "Append")
        .option("relationship.target.labels", ":Product")
        .save()
    // end::code-native[]
}

def keysStrategy() = {
    // tag::code-keys[]
    val relDF = Seq(
        ("John", "Doe", 1, "Product 1", 200, "ABC100"),
        ("Jane", "Doe", 2, "Product 2", 100, "ABC200")
    ).toDF("name", "surname", "customerID", "product", "quantity", "order")

    relDF.write
        // Create new relationships
        .mode("Append")
        .format("org.neo4j.spark.DataSource")
        // Assign a type to the relationships
        .option("relationship", "BOUGHT")
        // Use `keys` strategy
        .option("relationship.save.strategy", "keys")
        // Create source nodes and assign them a label
        .option("relationship.source.save.mode", "Append")
        .option("relationship.source.labels", ":Customer")
        // Map the DataFrame columns to node properties
        .option(
            "relationship.source.node.properties",
            "name,surname,customerID:id"
        )
        // Create target nodes and assign them a label
        .option("relationship.target.save.mode", "Append")
        .option("relationship.target.labels", ":Product")
        // Map the DataFrame columns to node properties
        .option("relationship.target.node.properties", "product:name")
        // Map the DataFrame columns to relationship properties
        .option("relationship.properties", "quantity,order")
        .save()
    // end::code-keys[]
}

def matchMode() = {
    // tag::code-match[]
    val relDF = Seq(
        ("John", "Doe", 1, "Product 1", 200, "ABC100"),
        ("Jane", "Doe", 2, "Product 2", 100, "ABC200")
    ).toDF("name", "surname", "customerID", "product", "quantity", "order")

    relDF.write
        // Create new relationships
        .mode(SaveMode.Append)
        .format("org.neo4j.spark.DataSource")
        // Assign a type to the relationships
        .option("relationship", "BOUGHT")
        // Use `keys` strategy
        .option("relationship.save.strategy", "keys")
        // Match source nodes with the specified label
        .option("relationship.source.save.mode", "Match")
        .option("relationship.source.labels", ":Customer")
        // Map the DataFrame columns to node properties
        .option(
            "relationship.source.node.properties",
            "name,surname,customerID:id"
        )
        // Node keys are mandatory for overwrite save mode
        .option("relationship.source.node.keys", "customerID:id")
        // Match target nodes with the specified label
        .option("relationship.target.save.mode", "Match")
        .option("relationship.target.labels", ":Product")
        // Map the DataFrame columns to node properties
        .option("relationship.target.node.properties", "product:name")
        // Node keys are mandatory for overwrite save mode
        .option("relationship.target.node.keys", "product:name")
        // Map the DataFrame columns to relationship properties
        .option("relationship.properties", "quantity,order")
        .save()
    // end::code-match[]
}

def overwriteMode() = {
    // tag::code-overwrite[]
    val relDF = Seq(
        ("John", "Doe", 1, "Product 1", 200, "ABC100"),
        ("Jane", "Doe", 2, "Product 2", 100, "ABC200")
    ).toDF("name", "surname", "customerID", "product", "quantity", "order")

    relDF.write
        // Create new relationships
        .mode(SaveMode.Append)
        .format("org.neo4j.spark.DataSource")
        // Assign a type to the relationships
        .option("relationship", "BOUGHT")
        // Use `keys` strategy
        .option("relationship.save.strategy", "keys")
        // Overwrite source nodes and assign them a label
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Customer")
        // Map the DataFrame columns to node properties
        .option(
            "relationship.source.node.properties",
            "name,surname,customerID:id"
        )
        // Node keys are mandatory for overwrite save mode
        .option("relationship.source.node.keys", "customerID:id")
        // Overwrite target nodes and assign them a label
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Product")
        // Map the DataFrame columns to node properties
        .option("relationship.target.node.properties", "product:name")
        // Node keys are mandatory for overwrite save mode
        .option("relationship.target.node.keys", "product:name")
        // Map the DataFrame columns to relationship properties
        .option("relationship.properties", "quantity,order")
        .save()
    // end::code-overwrite[]
}

def overwriteModeNodeRel() = {
    // tag::code-overwrite-node-rel[]
    val relDF = Seq(
        ("John", "Doe", 1, "Product 1", 200, "ABC100"),
        ("Jane", "Doe", 2, "Product 2", 100, "ABC200")
    ).toDF("name", "surname", "customerID", "product", "quantity", "order")

    relDF.write
        // Overwrite relationships
        .mode(SaveMode.Overwrite)
        .format("org.neo4j.spark.DataSource")
        // Assign a type to the relationships
        .option("relationship", "BOUGHT")
        // Use `keys` strategy
        .option("relationship.save.strategy", "keys")
        // Overwrite source nodes and assign them a label
        .option("relationship.source.save.mode", "Overwrite")
        .option("relationship.source.labels", ":Customer")
        // Map the DataFrame columns to node properties
        .option(
            "relationship.source.node.properties",
            "name,surname,customerID:id"
        )
        // Node keys are mandatory for overwrite save mode
        .option("relationship.source.node.keys", "customerID:id")
        // Overwrite target nodes and assign them a label
        .option("relationship.target.save.mode", "Overwrite")
        .option("relationship.target.labels", ":Product")
        // Map the DataFrame columns to node properties
        .option("relationship.target.node.properties", "product:name")
        // Node keys are mandatory for overwrite save mode
        .option("relationship.target.node.keys", "product:name")
        // Map the DataFrame columns to relationship properties
        .option("relationship.properties", "quantity,order")
        .save()
    // end::code-overwrite-node-rel[]
}

nativeStrategy()
keysStrategy()
matchMode()
overwriteMode()
overwriteModeNodeRel()
