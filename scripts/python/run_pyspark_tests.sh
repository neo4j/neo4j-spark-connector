#!/usr/bin/env bash

jar_path=$1
docker_image=$2
spark_version=$3

python test_pyspark_integration.py "$jar_path"  "$docker_image"
exit_code=$?

if [ $exit_code -ne 0 ]; then
    printf "%s\n" "test_pyspark_integration.py FAILED!!!"
    exit $exit_code
fi

python test_pyspark.py "$jar_path"
exit_code=$?

if [ $exit_code -ne 0 ]; then
    printf "%s\n" "test_pyspark.py FAILED!!!"
    exit $exit_code
fi

# spark declarative pipeline integration test:
if [[ "$spark_version" != 4.0.* ]]; then
    python pipeline/test_spark_declarative_pipeline.py "$jar_path" "$docker_image"
fi
