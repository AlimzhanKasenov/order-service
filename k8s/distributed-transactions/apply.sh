#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="distributed-transactions"

echo "Применение namespace и Secret..."

kubectl apply \
  -f "$ROOT_DIR/00-namespace.yaml" \
  -f "$ROOT_DIR/01-secrets.yaml"

echo "Развёртывание PostgreSQL..."

kubectl apply \
  -f "$ROOT_DIR/02-order-postgres.yaml" \
  -f "$ROOT_DIR/03-billing-postgres.yaml" \
  -f "$ROOT_DIR/04-notification-postgres.yaml" \
  -f "$ROOT_DIR/05-inventory-postgres.yaml" \
  -f "$ROOT_DIR/06-delivery-postgres.yaml"

echo "Развёртывание Kafka..."

kubectl apply \
  -f "$ROOT_DIR/07-kafka.yaml"

kubectl rollout status deployment/kafka \
  -n "$NAMESPACE" \
  --timeout=240s

echo "Создание Kafka topics..."

kubectl delete job kafka-init \
  -n "$NAMESPACE" \
  --ignore-not-found

kubectl apply \
  -f "$ROOT_DIR/08-kafka-init.yaml"

kubectl wait \
  --for=condition=complete \
  job/kafka-init \
  -n "$NAMESPACE" \
  --timeout=180s

echo "Развёртывание микросервисов..."

kubectl apply \
  -f "$ROOT_DIR/09-order-service.yaml" \
  -f "$ROOT_DIR/10-billing-service.yaml" \
  -f "$ROOT_DIR/11-notification-service.yaml" \
  -f "$ROOT_DIR/12-inventory-service.yaml" \
  -f "$ROOT_DIR/13-delivery-service.yaml"

echo "Развёртывание Ingress..."

kubectl apply \
  -f "$ROOT_DIR/14-ingress.yaml"

echo
echo "Готово."
echo

kubectl get pods -n "$NAMESPACE"
