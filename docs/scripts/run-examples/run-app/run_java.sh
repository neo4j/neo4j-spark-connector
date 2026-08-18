EXAMPLES_ROOT=../../../../modules/ROOT/examples
CONNECTOR_VERSION=$(grep -o "exact-connector-version: .\+" ../../../antora.yml | grep -o "\d\+\.\d\+\.\d\+")
SCALA_VERSION=$(grep -o " scala-version: .\+" ../../../antora.yml | grep -o "\d\+\.\d\+")
SPARK_VERSION=$(grep -o "spark-version: .\+" ../../../antora.yml | grep -o "\d\+\.\d\+\.\d\+")

mkdir -p java-example
cd java-example

cp $EXAMPLES_ROOT/java/pom.xml .
sed -i '' -e "s/{scala-version}/$SCALA_VERSION/g" pom.xml
sed -i '' -e "s/{spark-version}/$SPARK_VERSION/g" pom.xml
sed -i '' -e "s/{exact-connector-version}/$CONNECTOR_VERSION/g" pom.xml
cp $EXAMPLES_ROOT/example.jsonl .

mkdir -p src/main/java
cp $EXAMPLES_ROOT/java/SparkApp.java src/main/java/

mvn package

$SPARK_HOME/bin/spark-submit \
  --packages org.neo4j.connectors:spark:${CONNECTOR_VERSION}-s_${SCALA_VERSION} \
  --class SparkApp \
  target/spark-app-1.0.jar
