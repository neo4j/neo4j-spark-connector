#!/bin/bash

set -eEux pipefail

if [[ $# -lt 5 ]] ; then
    echo "Usage ./upload_to_spark_packages.sh <USER> <TOKEN> <GIT_HASH> <VERSION> <PATH_TO_PACKAGE_FILE>"
    exit 1
fi

USER=$1
TOKEN=$2
GIT_HASH=$3
VERSION=$4
PATH_TO_PACKAGE_FILE=$5

# License codes expected:
#   0 - Apache 2.0
#   1 - BSD 3-Clause
#   2 - BSD 2-Clause
#   3 - GPL-2.0
#   4 - GPL-3.0
#   5 - LGPL-2.1
#   6 - LGPL-3.0
#   7 - MIT
#   8 - MPL-2.0
#   9 - EPL-1.0
#   10 - Other license
LICENSE="0"

curl -X POST 'https://spark-packages.org/api/submit-release' \
  -u "$USER:$TOKEN" \
  -F "git_commit_sha1=$GIT_HASH" \
  -F "version=$VERSION" \
  -F "license_id=$LICENSE" \
  -F "name=neo4j-contrib/neo4j-spark-connector" \
  -F "artifact_zip=@$PATH_TO_PACKAGE_FILE"
