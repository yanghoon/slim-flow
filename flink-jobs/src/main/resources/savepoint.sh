#!/bin/bash

FLINK_URL="${FLINK_URL:-http://localhost:8081}"

curl -v -X POST "${FLINK_URL}/jobs/${1}/savepoints" \
    -H "Content-Type: application/json" \
    -d '{}'

# curl -v "${FLINK_URL}/jobs/${1}/savepoints"
# curl -v "${FLINK_URL}/jobs/${1}/savepoints/${2}"
