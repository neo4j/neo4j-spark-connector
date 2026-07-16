#!/bin/bash

set -eEuxo pipefail

if [[ $# -lt 2 ]] ; then
    echo "Usage ./maven-release.sh <GOAL> <SCALA-VERSION> [<ALT_DEPLOYMENT_REPOSITORY>]"
    exit 1
fi

exit_script() {
  echo "Process terminated cleaning up resources"
  mv -f pom.xml.bak pom.xml
  mv -f spark/pom.xml.bak spark/pom.xml
  mv -f spark-slim/pom.xml.bak spark-slim/pom.xml
  trap - SIGINT SIGTERM # clear the trap
  kill -- -$$ || true # Sends SIGTERM to child/sub processes
}

mvn_evaluate() {
  local expression
  expression="${1}"
  ./mvnw -B help:evaluate -Dexpression="${expression}" --quiet -DforceStdout
}

trap exit_script SIGINT SIGTERM

GOAL=$1
SCALA_VERSION=$2
if [[ $# -eq 3 ]] ; then
  ALT_DEPLOYMENT_REPOSITORY="-DaltDeploymentRepository=$3"
else
  ALT_DEPLOYMENT_REPOSITORY=""
fi

PROJECT_VERSION=$(mvn_evaluate "project.version")
RELEASE_VERSION="${PROJECT_VERSION}-s_$SCALA_VERSION"

# backup files
cp pom.xml pom.xml.bak
cp spark/pom.xml spark/pom.xml.bak
cp spark-slim/pom.xml spark-slim/pom.xml.bak

./mvnw -B versions:set -DnewVersion=${RELEASE_VERSION} -DgenerateBackupPoms=false

# build
./mvnw -B clean "${GOAL}" -Dscala-"${SCALA_VERSION}" -DskipTests ${ALT_DEPLOYMENT_REPOSITORY}

if [ ! ${CI:-false} = true ]; then
  exit_script
fi
