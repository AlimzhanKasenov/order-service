#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(
  cd "$(dirname "${BASH_SOURCE[0]}")"
  pwd
)"

PROJECT_DIR="$(
  cd "${SCRIPT_DIR}/.."
  pwd
)"

COLLECTION_FILE="${PROJECT_DIR}/postman/auth-bff.postman_collection.json"

RESULT_DIR="${PROJECT_DIR}/newman"

RESULT_FILE="${RESULT_DIR}/auth-bff-newman-result.txt"

BASE_URL="${BASE_URL:-http://arch.homework:8080}"

mkdir -p "${RESULT_DIR}"

echo "============================================================"
echo "Authentication and BFF - Newman"
echo "============================================================"
echo
echo "Collection:"
echo "${COLLECTION_FILE}"
echo
echo "baseUrl для текущего запуска:"
echo "${BASE_URL}"
echo
echo "Результат будет сохранён:"
echo "${RESULT_FILE}"
echo
echo "============================================================"
echo

newman run \
  "${COLLECTION_FILE}" \
  --env-var "baseUrl=${BASE_URL}" \
  --reporters cli \
  2>&1 | tee "${RESULT_FILE}"

echo
echo "============================================================"
echo "Newman завершён"
echo "============================================================"
echo
echo "Результат:"
echo "${RESULT_FILE}"