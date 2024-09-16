#!/bin/bash

set -eEuxo pipefail

if [[ $# -lt 2 ]] ; then
    echo "Usage ./maven-release.sh <GOAL> <SCALA-VERSION> [<ALT_DEPLOYMENT_REPOSITORY>]"
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

mvn_evaluate() {
  local expression
  expression="${1}"
  mvn help:evaluate -Dexpression="${expression}" --quiet -DforceStdout
}

trap exit_script SIGINT SIGTERM

GOAL=$1
SCALA_VERSION=$2
SPARK_VERSION=3
if [[ $# -eq 3 ]] ; then
  ALT_DEPLOYMENT_REPOSITORY="-DaltDeploymentRepository=$3"
else
  ALT_DEPLOYMENT_REPOSITORY=""
fi

case $(sed --help 2>&1) in
  *GNU*) sed_i () { sed -i "$@"; };;
  *) sed_i () { sed -i '' "$@"; };;
esac



PROJECT_VERSION=$(mvn_evaluate "project.version")
SPARK_PACKAGES_VERSION="${PROJECT_VERSION}-s_$SCALA_VERSION"

# backup files
cp pom.xml pom.xml.bak
cp common/pom.xml common/pom.xml.bak
cp test-support/pom.xml test-support/pom.xml.bak
cp spark-3/pom.xml spark-3/pom.xml.bak

mvn versions:set -DnewVersion=${PROJECT_VERSION}_for_spark_${SPARK_VERSION} -DgenerateBackupPoms=false

# replace pom files with target scala version
sed_i "s/<artifactId>neo4j-connector-apache-spark_parent<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_parent<\/artifactId>/" pom.xml
sed_i "s/<artifactId>neo4j-connector-apache-spark_parent<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_parent<\/artifactId>/" "test-support/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_test-support<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_test-support<\/artifactId>/" "test-support/pom.xml"

sed_i "s/<artifactId>neo4j-connector-apache-spark_common<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_common<\/artifactId>/" "common/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_parent<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_parent<\/artifactId>/" "common/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_test-support<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_test-support<\/artifactId>/" "common/pom.xml"

sed_i "s/<artifactId>neo4j-connector-apache-spark<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}<\/artifactId>/" "spark-3/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_parent<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_parent<\/artifactId>/" "spark-3/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_common<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_common<\/artifactId>/" "spark-3/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_test-support<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_test-support<\/artifactId>/" "spark-3/pom.xml"
sed_i "s/<spark-packages.version\/>/<spark-packages.version>${SPARK_PACKAGES_VERSION}<\/spark-packages.version>/" "spark-3/pom.xml"

# build
mvn clean "${GOAL}" -Dscala-"${SCALA_VERSION}" -DskipTests ${ALT_DEPLOYMENT_REPOSITORY}

if [ ! ${CI:-false} = true ]; then
  exit_script
fi
