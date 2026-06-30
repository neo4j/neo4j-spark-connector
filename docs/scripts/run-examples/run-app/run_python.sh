EXAMPLES_ROOT=../../../../modules/ROOT/examples
CONNECTOR_VERSION=$(grep -o "exact-connector-version: .\+" ../../../antora.yml | grep -o "\d\+\.\d\+\.\d\+")
SCALA_VERSION=$(grep -o " scala-version: .\+" ../../../antora.yml | grep -o "\d\+\.\d\+")

mkdir -p python-example
cd python-example

cp $EXAMPLES_ROOT/example.jsonl .
cp $EXAMPLES_ROOT/python/spark_app.py .

$SPARK_HOME/bin/spark-submit \
  --packages org.neo4j:neo4j-connector-apache-spark_${SCALA_VERSION}:${CONNECTOR_VERSION}_for_spark_3 \
  spark_app.py
