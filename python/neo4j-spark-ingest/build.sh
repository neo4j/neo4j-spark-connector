#!/usr/bin/env bash
set -euo pipefail

function fatal {
  local action="$1"

  printf "%bfatal:%b could not %s\n" '\033[0;31m' '\033[0m' "$action" >&2
  exit 1
}

if [[ $# -ne 1 || -z "${1:-}" ]]; then
  printf 'Usage: %s <version>\n' "$0" >&2
  exit 1
fi

version="$1"
project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(git -C "$project_dir" rev-parse --show-toplevel)"

git -C "$project_dir" clean -fdx -- .
printf '%s\n' "$version" >"${project_dir}/VERSION"

"${repo_root}/mvnw" -f "${repo_root}/pom.xml" \
  -pl spark-ingest -am \
  -DskipTests -DskipITs \
  package

portable_jars=("${repo_root}"/spark-ingest/target/spark-ingest-*-portable.jar)
if ((${#portable_jars[@]} != 1)) || [[ ! -f "${portable_jars[0]}" ]]; then
  printf 'Expected exactly one portable spark-ingest JAR, found %s\n' "${#portable_jars[@]}" >&2
  exit 1
fi

mkdir -p "${project_dir}/src/neo4j_spark_ingest/lib"

if ! cp "${portable_jars[0]}" "${project_dir}/src/neo4j_spark_ingest/lib/neo4j-spark-ingest-portable.jar"; then
  fatal "move maven output to python library"
fi

if ! cp "${repo_root}/LICENSE.txt" "${project_dir}/LICENSE.txt"; then
  fatal "move repo root LICENSE.txt to python library"
fi

if ! cp "${repo_root}/spark-ingest/NOTICE.txt" "${project_dir}/NOTICE.txt"; then
  fatal "move spark-ingest/NOTICE.txt to python library"
fi

if ! cp "${repo_root}/spark-ingest/LICENSES.txt" "${project_dir}/LICENSES.txt"; then
  fatal "move spark-ingest/LICENSES.txt to python library"
fi

if ! python3 -m pip wheel --no-deps --wheel-dir "${project_dir}/dist" "${project_dir}"; then
  fatal "build the wheel, check output!"
fi

wheels=("${project_dir}"/dist/*.whl)
if ((${#wheels[@]} != 1)) || [[ ! -f "${wheels[0]}" ]]; then
  fatal "select exactly one wheel from dist"
fi

test_dir="$(mktemp -d)"
trap 'rm -rf -- "$test_dir"' EXIT
unzip -q "${wheels[0]}" -d "$test_dir"

if ! PYTHONPATH="$test_dir" python3 "${project_dir}/tests/test_plan.py"; then
  fatal "pass smoke tests"
fi

printf "success!\n"
exit 0
