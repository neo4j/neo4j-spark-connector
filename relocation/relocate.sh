#!/bin/env bash

if ! test -d "spark-3"; then
  echo "you are likely not in repo root, please run this script from repo root"
  exit 1
fi

set -euo pipefail

repository="${PWD}/target/relocation-repository"

rm -rf "${repository}"

./mvnw -B -f relocation/scala_2.13/6.0.0_for_spark_3/pom.xml deploy -DaltDeploymentRepository="relocation::file://${repository}"
./mvnw -B -f relocation/scala_2.13/6.0.0_for_spark_4/pom.xml deploy -DaltDeploymentRepository="relocation::file://${repository}"
