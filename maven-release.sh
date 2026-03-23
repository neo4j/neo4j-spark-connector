#!/bin/bash

set -eEuxo pipefail

if [[ $# -lt 2 ]] ; then
    echo "Usage ./maven-release.sh <GOAL> <SCALA-VERSION> [<ALT_DEPLOYMENT_REPOSITORY>]"
    echo ""
    echo "SCALA-VERSION:"
    echo "  2.12  — sets -Dscala-2.12=true (activates the scala-2.12 Maven profile)"
    echo "  2.13  — sets -Dscala-2.13=true (activates the scala-2.13 Maven profile)"
    echo "  3     — passes -Pscala-3-poc (your Scala 3 POC profile; artifact suffix _3_)"
    echo ""
    echo "Override: set MAVEN_PROFILES to a comma-separated list of profile ids, e.g."
    echo "  MAVEN_PROFILES=scala-3-poc ./maven-release.sh package 3"
    exit 1
fi

exit_script() {
  echo "Process terminated cleaning up resources"
  mv -f pom.xml.bak pom.xml
  mv -f common/pom.xml.bak common/pom.xml
  mv -f test-support/pom.xml.bak test-support/pom.xml
  mv -f spark/pom.xml.bak spark/pom.xml
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
SPARK_VERSION=4
# Default id for the Scala 3 POC profile in pom.xml (change if you rename the profile)
SCALA3_POC_PROFILE_ID="${SCALA3_POC_PROFILE_ID:-scala-3-poc}"

if [[ $# -eq 3 ]] ; then
  ALT_DEPLOYMENT_REPOSITORY="-DaltDeploymentRepository=$3"
else
  ALT_DEPLOYMENT_REPOSITORY=""
fi

# How we activate Scala-specific Maven state must match pom.xml:
# - scala-2.12 profile: property activation on "scala-2.12"
# - scala-2.13 profile: property activation on "scala-2.13"
# - scala-3-poc profile: explicit -P (no property activation)
if [[ -n "${MAVEN_PROFILES:-}" ]]; then
  MAVEN_SCALA_OPTS=(-P"${MAVEN_PROFILES}")
else
  case "${SCALA_VERSION}" in
    2.12)
      MAVEN_SCALA_OPTS=(-Dscala-2.12.=true)
      ;;
    2.13)
      MAVEN_SCALA_OPTS=(-Dscala-2.13=true)
      ;;
    3)
      MAVEN_SCALA_OPTS=(-P"${SCALA3_POC_PROFILE_ID}")
      ;;
    *)
      echo "Unsupported SCALA_VERSION '${SCALA_VERSION}'. Use 2.12, 2.13 or 3, or set MAVEN_PROFILES." >&2
      exit 1
      ;;
  esac
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
cp spark/pom.xml spark/pom.xml.bak

./mvnw -B versions:set -DnewVersion=${PROJECT_VERSION}_for_spark_${SPARK_VERSION} -DgenerateBackupPoms=false

# replace pom files with target scala version
sed_i "s/<artifactId>neo4j-connector-apache-spark_parent<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_parent<\/artifactId>/" pom.xml
sed_i "s/<artifactId>neo4j-connector-apache-spark_parent<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_parent<\/artifactId>/" "test-support/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_test-support<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_test-support<\/artifactId>/" "test-support/pom.xml"

sed_i "s/<artifactId>neo4j-connector-apache-spark_common<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_common<\/artifactId>/" "common/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_parent<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_parent<\/artifactId>/" "common/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_test-support<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_test-support<\/artifactId>/" "common/pom.xml"

sed_i "s/<artifactId>neo4j-connector-apache-spark<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}<\/artifactId>/" "spark/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_parent<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_parent<\/artifactId>/" "spark/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_common<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_common<\/artifactId>/" "spark/pom.xml"
sed_i "s/<artifactId>neo4j-connector-apache-spark_test-support<\/artifactId>/<artifactId>neo4j-connector-apache-spark_${SCALA_VERSION}_test-support<\/artifactId>/" "spark/pom.xml"
sed_i "s/<spark-packages.version\/>/<spark-packages.version>${SPARK_PACKAGES_VERSION}<\/spark-packages.version>/" "spark/pom.xml"

# build
./mvnw -B clean "${GOAL}" "${MAVEN_SCALA_OPTS[@]}" -DskipTests ${ALT_DEPLOYMENT_REPOSITORY} -Doverwrite

if [ ! ${CI:-false} = true ]; then
  exit_script
fi
