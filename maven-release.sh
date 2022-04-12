if [[ $# -lt 3 ]] ; then
    echo "Usage ./maven-release.sh <DEPLOY_OR_INSTALL> <SCALA-VERSION> <SPARK-VERSION> [<ALT_DEPLOYMENT_REPOSITORY>]"
    exit 1
fi

JAVA_VER=$(java -version 2>&1 | grep -i version)

if [[ ! $JAVA_VER =~ 1.8 ]] ; then
    echo "You must use Java 8"
    exit 1
fi

exit_script() {
  echo "Process terminated cleaning up resources"
  mv -f pom.xml.bak pom.xml
  mv -f common/pom.xml.bak common/pom.xml
  mv -f doc/pom.xml.bak doc/pom.xml
  mv -f test-support/pom.xml.bak test-support/pom.xml
  mv -f "${TARGET_DIR}/pom.xml.bak" "${TARGET_DIR}/pom.xml"
  trap - SIGINT SIGTERM # clear the trap
  kill -- -$$ # Sends SIGTERM to child/sub processes
}

trap exit_script SIGINT SIGTERM

DEPLOY_INSTALL=$1
SCALA_VERSION=$2
SPARK_VERSION=$3
TARGET_DIR=spark-$SPARK_VERSION
if [[ $# -eq 4 ]] ; then
  ALT_DEPLOYMENT_REPOSITORY="-DaltDeploymentRepository=$4"
else
  ALT_DEPLOYMENT_REPOSITORY=""
fi

case $(sed --help 2>&1) in
  *GNU*) sed_i () { sed -i "$@"; };;
  *) sed_i () { sed -i '' "$@"; };;
esac

# backup files
cp pom.xml pom.xml.bak
cp common/pom.xml common/pom.xml.bak
cp doc/pom.xml doc/pom.xml.bak
cp test-support/pom.xml test-support/pom.xml.bak
cp "${TARGET_DIR}/pom.xml" "${TARGET_DIR}/pom.xml.bak"

# replace pom files with target scala version
sed_i "s/<artifactId>neo4j-connector-apache-spark<\/artifactId>/<artifactId>neo4j-connector-apache-spark_$SCALA_VERSION<\/artifactId>/" pom.xml
sed_i "s/<scala.binary.version \/>/<scala.binary.version>$SCALA_VERSION<\/scala.binary.version>/" pom.xml
sed_i "s/<artifactId>neo4j-connector-apache-spark<\/artifactId>/<artifactId>neo4j-connector-apache-spark_$SCALA_VERSION<\/artifactId>/" "doc/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark<\/artifactId>/<artifactId>neo4j-connector-apache-spark_$SCALA_VERSION<\/artifactId>/" "common/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark<\/artifactId>/<artifactId>neo4j-connector-apache-spark_$SCALA_VERSION<\/artifactId>/" "test-support/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark<\/artifactId>/<artifactId>neo4j-connector-apache-spark_$SCALA_VERSION<\/artifactId>/" "${TARGET_DIR}/pom.xml"

# build
./mvnw clean $DEPLOY_INSTALL -pl !'doc' -Pscala-$SCALA_VERSION -Pspark-$SPARK_VERSION -DskipTests $ALT_DEPLOYMENT_REPOSITORY

exit_script
