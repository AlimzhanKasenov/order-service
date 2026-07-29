#!/usr/bin/env bash

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Создаём namespace logging"
kubectl apply -f "${DIR}/00-namespace.yaml"

echo "Устанавливаем Elasticsearch"
kubectl apply \
  -f "${DIR}/01-elasticsearch-services.yaml" \
  -f "${DIR}/02-elasticsearch-statefulset.yaml"

kubectl rollout status \
  statefulset/elasticsearch \
  -n logging \
  --timeout=600s

echo "Устанавливаем Kibana"
kubectl apply -f "${DIR}/03-kibana.yaml"

kubectl rollout status \
  deployment/kibana \
  -n logging \
  --timeout=600s

echo "Устанавливаем Fluent Bit"
kubectl apply \
  -f "${DIR}/04-fluent-bit-rbac.yaml" \
  -f "${DIR}/05-fluent-bit-configmap.yaml" \
  -f "${DIR}/06-fluent-bit-daemonset.yaml"

kubectl rollout status \
  daemonset/fluent-bit \
  -n logging \
  --timeout=300s

echo
echo "EFK-стек установлен"
kubectl get pods -n logging
kubectl get services -n logging
kubectl get pvc -n logging

echo
echo "Для открытия Kibana:"
echo "kubectl port-forward -n logging service/kibana-service 5601:5601"
echo "http://localhost:5601"
