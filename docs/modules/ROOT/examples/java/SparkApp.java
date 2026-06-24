import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkApp {
    public static void main(String[] args) {
        // Replace with the actual connection URI and credentials
        String url = "neo4j://localhost:7687";
        String username = "neo4j";
        String password = "password";
        String dbname = "neo4j";

        SparkSession spark = SparkSession
            .builder()
            .appName("Spark App")
            .config("neo4j.url", url)
            .config("neo4j.authentication.basic.username", username)
            .config("neo4j.authentication.basic.password", password)
            .config("neo4j.database", dbname)
            .getOrCreate();

        Dataset<Row> data = spark.read().json("example.jsonl");

        data.write().format("org.neo4j.spark.DataSource")
            .mode(SaveMode.Overwrite)
            .option("labels", "Person")
            .option("node.keys", "name,surname")
            .save();

        Dataset<Row> ds = spark.read().format("org.neo4j.spark.DataSource")
            .option("labels", "Person")
            .load();

        ds.show();
    }
}
