if [[ $# -lt 3 ]] ; then
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
SCALA_VERSION=$2
VERSION=$1_for_spark_$SPARK_VERSION
PKG_TARGET_DIR=spark-$SPARK_VERSION/target
./mvnw clean install -Pscala-$SCALA_VERSION -Pspark-$SPARK_VERSION -DskipTests
cat << EOF > $PKG_TARGET_DIR/$ARTIFACT-$VERSION.pom
<project>
<modelVersion>4.0.0</modelVersion>
<groupId>org.neo4j</groupId>
<artifactId>$ARTIFACT</artifactId>
<version>$VERSION</version>
</project>
EOF
cp pom.xml target/$ARTIFACT-$VERSION.pom
cp $PKG_TARGET_DIR/$ARTIFACT-$VERSION.jar target/$ARTIFACT-$VERSION.jar
zip -jv target/$ARTIFACT-$VERSION.zip target/$ARTIFACT-$VERSION.pom target/$ARTIFACT-$VERSION.jar
if ! command -v xdg-open &> /dev/null
then
    open target
else
    xdg-open target
fi
