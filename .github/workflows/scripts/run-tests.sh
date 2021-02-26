if [[ $# -lt 3 ]] ; then
  echo "Usage ./run-tests.sh <SCALA-VERSION> <SPARK-VERSION> <NEO4J-VERSION>"
  exit 1
fi

if [[ $2 == 3 ]] ; then
  SPARK_VERSION=3.0
else
  SPARK_VERSION=$2
fi

SCALA_VERSION=$1

if [[ $3 == 4 ]] ; then
  NEO4J_VERSION=4.0
else
  NEO4J_VERSION=$3
fi

if [[ $SCALA_VERSION == 2.11 && $SPARK_VERSION == 3.0 ]] ; then
    echo "Spark 3.0 does not support Scala 2.11, test are skipped"
    exit 0
fi

echo "*** Executing tests with Spark $SCALA_VERSION, Scala $SPARK_VERSION and Neo4j $NEO4J_VERSION"
./mvnw clean verify -P scala-$SCALA_VERSION -P spark-$SPARK_VERSION -P neo4j-$NEO4J_VERSION --no-transfer-progress
