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

def nodesMapFalse() = {
    // tag::code-nodes-map-false[]
    val df = spark.read
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "BOUGHT")
        // It can be omitted, since `false` is the default
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Customer")
        .option("relationship.target.labels", ":Product")
        .load()

    df.show()
    // end::code-nodes-map-false[]
}

def nodesMapTrue() = {
    // tag::code-nodes-map-true[]
    val df = spark.read
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "BOUGHT")
        .option("relationship.nodes.map", "true")
        .option("relationship.source.labels", ":Customer")
        .option("relationship.target.labels", ":Product")
        .load()

    // Use `false` to print the whole DataFrame
    df.show(false)
    // end::code-nodes-map-true[]
}

def nodesMapFalseFilter() = {
    // tag::code-nodes-map-false-filter[]
    val df = spark.read
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "BOUGHT")
        .option("relationship.nodes.map", "false")
        .option("relationship.source.labels", ":Customer")
        .option("relationship.target.labels", ":Product")
        .load()

    df.where("`source.id` > 1").show()
    // end::code-nodes-map-false-filter[]
}

def nodesMapTrueFilter() = {
    // tag::code-nodes-map-true-filter[]
    val df = spark.read
        .format("org.neo4j.spark.DataSource")
        .option("relationship", "BOUGHT")
        .option("relationship.nodes.map", "true")
        .option("relationship.source.labels", ":Customer")
        .option("relationship.target.labels", ":Product")
        .load()

    // Use `false` to print the whole DataFrame
    df.where("`<source>`.`id` > 1").show(false)
    // end::code-nodes-map-true-filter[]
}

nodesMapFalse()
nodesMapTrue()
nodesMapFalseFilter()
nodesMapTrueFilter()
