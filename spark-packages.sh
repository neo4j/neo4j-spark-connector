if [[ $# -lt 2 ]] ; then
    echo "Usage ./spark-packages.sh <VERSION> <SCALA-VERSION> <SPARK-VERSION>"
    exit 1
fi

JAVA_VER=$(java -version 2>&1 | grep -i version)

if [[ ! $JAVA_VER =~ 1.8 ]] ; then
    echo "You must use Java 8"
    exit 1
fi

ARTIFACT=neo4j-connector-apache-spark_$2
SPARK_VERSION=$3
VERSION=$1_for_spark_$SPARK_VERSION
./mvnw clean install -Pscala-$2 -DskipTests
cat << EOF > target/$ARTIFACT-$VERSION.pom
<project>
<modelVersion>4.0.0</modelVersion>
<groupId>org.neo4j</groupId>
<artifactId>$ARTIFACT</artifactId>
<version>$VERSION</version>
</project>
EOF
cp pom.xml target/$ARTIFACT-$VERSION.pom
cp spark-$SPARK_VERSION/target/$ARTIFACT-$VERSION.jar target/$ARTIFACT-$VERSION.jar
zip -jv target/$ARTIFACT-$VERSION.zip target/$ARTIFACT-$VERSION.pom target/$ARTIFACT-$VERSION.jar
xdg-open target