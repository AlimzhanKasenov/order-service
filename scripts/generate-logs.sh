#!/usr/bin/env bash

set -euo pipefail

PORT="${PORT:-18000}"
BASE_URL="http://127.0.0.1:${PORT}"
TMP_DIR="$(mktemp -d)"
RESPONSE_FILE="${TMP_DIR}/response.json"

cleanup()
{
  if [ -n "${PORT_FORWARD_PID:-}" ]; then
    kill "${PORT_FORWARD_PID}" 2>/dev/null || true
    wait "${PORT_FORWARD_PID}" 2>/dev/null || true
  fi

  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT

show_response()
{
  local status="$1"

  echo "HTTP ${status}"

  if [ -s "${RESPONSE_FILE}" ]; then
    python3 -m json.tool "${RESPONSE_FILE}" 2>/dev/null ||
      cat "${RESPONSE_FILE}"
  else
    echo "<пустой ответ>"
  fi
}

echo "Запускаем port-forward..."

kubectl port-forward \
  service/order-service \
  "${PORT}:80" \
  >"${TMP_DIR}/port-forward.log" \
  2>&1 &

PORT_FORWARD_PID=$!

for attempt in $(seq 1 30); do
  if curl -fsS "${BASE_URL}/health" >/dev/null 2>&1; then
    break
  fi

  if [ "${attempt}" -eq 30 ]; then
    echo "order-service недоступен"
    cat "${TMP_DIR}/port-forward.log"
    exit 1
  fi

  sleep 1
done

SUFFIX="$(date +%s)"
USERNAME="logging-user-${SUFFIX}"
EMAIL="logging-${SUFFIX}@example.com"

echo
echo "1. CREATE — INFO user_created"

STATUS="$(
  curl -sS \
    -o "${RESPONSE_FILE}" \
    -w '%{http_code}' \
    -X POST \
    -H 'Content-Type: application/json' \
    -d "{
      \"username\":\"${USERNAME}\",
      \"firstName\":\"Logging\",
      \"lastName\":\"Homework\",
      \"email\":\"${EMAIL}\",
      \"phone\":\"+77010000000\"
    }" \
    "${BASE_URL}/user"
)"

show_response "${STATUS}"

if [ "${STATUS}" != "201" ]; then
  echo "Не удалось создать пользователя"
  exit 1
fi

USER_ID="$(
  python3 - "${RESPONSE_FILE}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as file:
    data = json.load(file)

print(data["id"])
PY
)"

echo "${USER_ID}" > .last-generated-user-id
echo "Создан USER_ID=${USER_ID}"

echo
echo "2. READ — INFO user_read"

STATUS="$(
  curl -sS \
    -o "${RESPONSE_FILE}" \
    -w '%{http_code}' \
    "${BASE_URL}/user/${USER_ID}"
)"

show_response "${STATUS}"

echo
echo "3. UPDATE — INFO user_updated"

STATUS="$(
  curl -sS \
    -o "${RESPONSE_FILE}" \
    -w '%{http_code}' \
    -X PUT \
    -H 'Content-Type: application/json' \
    -d '{
      "firstName":"Updated",
      "phone":"+77011111111"
    }' \
    "${BASE_URL}/user/${USER_ID}"
)"

show_response "${STATUS}"

echo
echo "4. LIST — INFO users_listed"

STATUS="$(
  curl -sS \
    -o "${RESPONSE_FILE}" \
    -w '%{http_code}' \
    "${BASE_URL}/user"
)"

show_response "${STATUS}"

echo
echo "5. INVALID DATA — WARN validation_error"

STATUS="$(
  curl -sS \
    -o "${RESPONSE_FILE}" \
    -w '%{http_code}' \
    -X POST \
    -H 'Content-Type: application/json' \
    -d "{
      \"username\":\"invalid-${SUFFIX}\",
      \"firstName\":\"Invalid\",
      \"lastName\":\"Email\",
      \"email\":\"wrong-email\",
      \"phone\":\"+77012222222\"
    }" \
    "${BASE_URL}/user"
)"

show_response "${STATUS}"

echo
echo "6. EMPTY UPDATE — WARN validation_error"

STATUS="$(
  curl -sS \
    -o "${RESPONSE_FILE}" \
    -w '%{http_code}' \
    -X PUT \
    -H 'Content-Type: application/json' \
    -d '{}' \
    "${BASE_URL}/user/${USER_ID}"
)"

show_response "${STATUS}"

echo
echo "7. NOT FOUND — WARN resource_not_found"

STATUS="$(
  curl -sS \
    -o "${RESPONSE_FILE}" \
    -w '%{http_code}' \
    "${BASE_URL}/user/999999999"
)"

show_response "${STATUS}"

echo
echo "8. TEST ERROR — ERROR application_error"

STATUS="$(
  curl -sS \
    -o "${RESPONSE_FILE}" \
    -w '%{http_code}' \
    "${BASE_URL}/debug/error"
)"

show_response "${STATUS}"

echo
echo "9. DELETE — INFO user_deleted"

STATUS="$(
  curl -sS \
    -o "${RESPONSE_FILE}" \
    -w '%{http_code}' \
    -X DELETE \
    "${BASE_URL}/user/${USER_ID}"
)"

show_response "${STATUS}"

echo
echo "10. DELETED USER — WARN resource_not_found"

STATUS="$(
  curl -sS \
    -o "${RESPONSE_FILE}" \
    -w '%{http_code}' \
    "${BASE_URL}/user/${USER_ID}"
)"

show_response "${STATUS}"

echo
echo "=============================================="
echo "Генерация логов завершена"
echo "USER_ID=${USER_ID}"
echo "=============================================="
