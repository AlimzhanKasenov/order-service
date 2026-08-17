#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="stream-processing"

echo "========================================"
echo "Stream Processing deployment"
echo "========================================"

echo
echo "1. Создание namespace..."
kubectl apply -f "$ROOT_DIR/00-namespace.yaml"

echo
echo "2. Создание Secret..."
kubectl apply -f "$ROOT_DIR/01-secrets.yaml"

echo
echo "3. Запуск PostgreSQL..."
kubectl apply -f "$ROOT_DIR/02-order-postgres.yaml"
kubectl apply -f "$ROOT_DIR/03-billing-postgres.yaml"
kubectl apply -f "$ROOT_DIR/04-notification-postgres.yaml"

echo
echo "Ожидание готовности PostgreSQL..."

kubectl rollout status deployment/order-postgres \
  -n "$NAMESPACE" \
  --timeout=180s

kubectl rollout status deployment/billing-postgres \
  -n "$NAMESPACE" \
  --timeout=180s

kubectl rollout status deployment/notification-postgres \
  -n "$NAMESPACE" \
  --timeout=180s

echo
echo "PostgreSQL готов."

echo
echo "4. Запуск Kafka..."
kubectl apply -f "$ROOT_DIR/05-kafka.yaml"

echo
echo "Ожидание готовности Kafka..."

kubectl rollout status deployment/kafka \
  -n "$NAMESPACE" \
  --timeout=240s

echo
echo "Kafka готова."

echo
echo "5. Запуск микросервисов..."

kubectl apply -f "$ROOT_DIR/06-order-service.yaml"
kubectl apply -f "$ROOT_DIR/07-billing-service.yaml"
kubectl apply -f "$ROOT_DIR/08-notification-service.yaml"

echo
echo "Ожидание готовности микросервисов..."

kubectl rollout status deployment/order-service \
  -n "$NAMESPACE" \
  --timeout=180s

kubectl rollout status deployment/billing-service \
  -n "$NAMESPACE" \
  --timeout=180s

kubectl rollout status deployment/notification-service \
  -n "$NAMESPACE" \
  --timeout=180s

echo
echo "Микросервисы готовы."

echo
echo "6. Создание Ingress..."

if ! kubectl apply -f "$ROOT_DIR/09-ingress.yaml"; then
  echo
  echo "Ingress пока не создан."
  echo "Возможно, ingress-nginx admission webhook ещё не готов."
  echo "Повторная попытка через 10 секунд..."
  sleep 10

  kubectl apply -f "$ROOT_DIR/09-ingress.yaml"
fi

echo
echo "========================================"
echo "Deployment завершён"
echo "========================================"

echo
echo "Pods:"
kubectl get pods -n "$NAMESPACE"

echo
echo "Services:"
kubectl get svc -n "$NAMESPACE"

echo
echo "Ingress:"
kubectl get ingress -n "$NAMESPACE" || true

echo
echo "Kafka topics:"

KAFKA_POD="$(kubectl get pod \
  -n "$NAMESPACE" \
  -l app=kafka \
  -o jsonpath='{.items[0].metadata.name}')"

kubectl exec \
  -n "$NAMESPACE" \
  "$KAFKA_POD" \
  -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --list

echo
echo "Готово."
