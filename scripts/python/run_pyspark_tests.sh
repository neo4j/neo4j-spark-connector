#!/usr/bin/env bash

python test_pyspark_integration.py "$1" "$2"
test_exit_code=$?

if [ $test_exit_code -ne 0 ]; then
    printf "%s\n" "test_pyspark_integration.py FAILED!!!"
    exit $test_exit_code
fi

python test_pyspark.py "$1"
test_exit_code=$?

if [ $test_exit_code -ne 0 ]; then
    printf "%s\n" "test_pyspark.py FAILED!!!"
    exit $test_exit_code
fi
