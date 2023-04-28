#!/bin/bash

set -eEuxo pipefail

if [[ $# -lt 2 ]] ; then
    echo "Usage ./maven-release.sh <DEPLOY_OR_INSTALL> <SCALA-VERSION> [<ALT_DEPLOYMENT_REPOSITORY>]"
    exit 1
fi

exit_script() {
  echo "Process terminated cleaning up resources"
  mv -f pom.xml.bak pom.xml
  mv -f common/pom.xml.bak common/pom.xml
  mv -f test-support/pom.xml.bak test-support/pom.xml
  mv -f spark-3/pom.xml.bak spark-3/pom.xml
  trap - SIGINT SIGTERM # clear the trap
  kill -- -$$ # Sends SIGTERM to child/sub processes
}

trap exit_script SIGINT SIGTERM

DEPLOY_INSTALL=$1
SCALA_VERSION=$2
if [[ $# -eq 3 ]] ; then
  ALT_DEPLOYMENT_REPOSITORY="-DaltDeploymentRepository=$3"
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
cp test-support/pom.xml test-support/pom.xml.bak
cp spark-3/pom.xml spark-3/pom.xml.bak

# replace pom files with target scala version
sed_i "s/<artifactId>neo4j-connector-apache-spark<\/artifactId>/<artifactId>neo4j-connector-apache-spark_$SCALA_VERSION<\/artifactId>/" pom.xml
sed_i "s/<scala.binary.version \/>/<scala.binary.version>$SCALA_VERSION<\/scala.binary.version>/" pom.xml
sed_i "s/<artifactId>neo4j-connector-apache-spark<\/artifactId>/<artifactId>neo4j-connector-apache-spark_$SCALA_VERSION<\/artifactId>/" "common/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark<\/artifactId>/<artifactId>neo4j-connector-apache-spark_$SCALA_VERSION<\/artifactId>/" "test-support/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark<\/artifactId>/<artifactId>neo4j-connector-apache-spark_$SCALA_VERSION<\/artifactId>/" "spark-3/pom.xml"

# build
mvn clean "${DEPLOY_INSTALL}" -Pscala-"${SCALA_VERSION}" -DskipTests ${ALT_DEPLOYMENT_REPOSITORY}

exit_script
