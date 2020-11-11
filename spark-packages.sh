if [[ $# -lt 2 ]] ; then
    echo "Usage ./spark-packages.sh <VERSION> <SCALA-VERSION>"
    exit 1
fi

ARTIFACT=neo4j-connector-apache-spark_$2
VERSION=$1
./mvnw clean install -Pscala-$2 -DskipTests
cat << EOF > target/$ARTIFACT-$VERSION.pom
<project>
<modelVersion>4.0.0</modelVersion>
<groupId>neo4j-contrib</groupId>
<artifactId>$ARTIFACT</artifactId>
<version>$VERSION</version>
</project>
EOF
cp pom.xml target/$ARTIFACT-$VERSION.pom
cp target/$ARTIFACT-$VERSION.jar target/$ARTIFACT-$VERSION.jar
zip -jv target/$ARTIFACT-$VERSION.zip target/$ARTIFACT-$VERSION.pom target/$ARTIFACT-$VERSION.jar
xdg-open target