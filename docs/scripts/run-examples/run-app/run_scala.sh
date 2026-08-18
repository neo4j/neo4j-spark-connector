EXAMPLES_ROOT=../../../../modules/ROOT/examples
CONNECTOR_VERSION=$(grep -o "exact-connector-version: .\+" ../../../antora.yml | grep -o "\d\+\.\d\+\.\d\+")
EXACT_SCALA_VERSION=$(grep -o "exact-scala-version: .\+" ../../../antora.yml | grep -o "\d\+\.\d\+\.\d\+")
SCALA_VERSION=$(grep -o " scala-version: .\+" ../../../antora.yml | grep -o "\d\+\.\d\+")
SPARK_VERSION=$(grep -o "spark-version: .\+" ../../../antora.yml | grep -o "\d\+\.\d\+\.\d\+")

mkdir -p scala-example
cd scala-example

cp $EXAMPLES_ROOT/scala/build.sbt .
sed -i '' -e "s/{exact-scala-version}/$EXACT_SCALA_VERSION/g" build.sbt
sed -i '' -e "s/{spark-version}/$SPARK_VERSION/g" build.sbt
sed -i '' -e "s/{exact-connector-version}/$CONNECTOR_VERSION/g" build.sbt
cp $EXAMPLES_ROOT/example.jsonl .

mkdir -p src/main/scala
cp $EXAMPLES_ROOT/scala/SparkApp.scala src/main/scala/

sbt package

$SPARK_HOME/bin/spark-submit \
  --packages org.neo4j.connectors:spark:${CONNECTOR_VERSION}-s_${SCALA_VERSION} \
  --class SparkApp \
  target/scala-${SCALA_VERSION}/spark-app_${SCALA_VERSION}-1.0.jar
