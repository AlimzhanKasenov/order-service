#!/usr/bin/env bash

set -euo pipefail

# Получаем абсолютный путь к директории k8s,
# чтобы скрипт можно было запускать из любого места.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

POSTGRES_RELEASE="order-db"
POSTGRES_CHART="oci://registry-1.docker.io/bitnamicharts/postgresql"

echo "============================================================"
echo "1. Создаём Secret с доступами к PostgreSQL"
echo "============================================================"

kubectl apply -f "${SCRIPT_DIR}/01-secret.yaml"

echo
echo "============================================================"
echo "2. Проверяем доступность Helm-чарта PostgreSQL"
echo "============================================================"

helm show chart "${POSTGRES_CHART}" | head -20

echo
echo "============================================================"
echo "3. Устанавливаем или обновляем PostgreSQL через Helm"
echo "============================================================"

# Версию чарта намеренно не фиксируем:
# Helm скачает текущую доступную стабильную версию из OCI-реестра.
helm upgrade --install "${POSTGRES_RELEASE}" "${POSTGRES_CHART}" \
  -f "${SCRIPT_DIR}/postgres-values.yaml"

echo
echo "============================================================"
echo "4. Ожидаем создания Pod PostgreSQL"
echo "============================================================"

# Иногда Pod появляется не мгновенно после завершения Helm.
for attempt in $(seq 1 30); do
  if kubectl get pods \
    -l app.kubernetes.io/instance="${POSTGRES_RELEASE}" \
    --no-headers 2>/dev/null | grep -q .; then
    echo "Pod PostgreSQL найден."
    break
  fi

  if [ "${attempt}" -eq 30 ]; then
    echo "Ошибка: Pod PostgreSQL не появился."
    kubectl get pods
    exit 1
  fi

  echo "Ожидаем появление Pod PostgreSQL: попытка ${attempt} из 30..."
  sleep 2
done

echo
echo "============================================================"
echo "5. Ожидаем готовность PostgreSQL"
echo "============================================================"

kubectl wait \
  --for=condition=Ready \
  pod \
  -l app.kubernetes.io/instance="${POSTGRES_RELEASE}" \
  --timeout=300s

echo
echo "============================================================"
echo "6. Применяем ConfigMap приложения и миграции"
echo "============================================================"

kubectl apply \
  -f "${SCRIPT_DIR}/02-configmap.yaml" \
  -f "${SCRIPT_DIR}/03-migration-configmap.yaml"

echo
echo "============================================================"
echo "7. Запускаем первоначальную миграцию"
echo "============================================================"

# Job нельзя обновлять после создания,
# поэтому перед повторным запуском удаляем старую Job.
kubectl delete job order-service-migration --ignore-not-found

kubectl apply -f "${SCRIPT_DIR}/04-migration-job.yaml"

echo
echo "============================================================"
echo "8. Ожидаем завершение миграции"
echo "============================================================"

if ! kubectl wait \
  --for=condition=complete \
  job/order-service-migration \
  --timeout=180s; then

  echo "Миграция завершилась с ошибкой."
  echo
  kubectl describe job order-service-migration
  echo
  kubectl logs job/order-service-migration || true
  exit 1
fi

echo
echo "Лог миграции:"
kubectl logs job/order-service-migration

echo
echo "============================================================"
echo "9. Запускаем приложение, Service и Ingress"
echo "============================================================"

kubectl apply \
  -f "${SCRIPT_DIR}/00-ingress-class.yaml" \
  -f "${SCRIPT_DIR}/05-deployment.yaml" \
  -f "${SCRIPT_DIR}/06-service.yaml" \
  -f "${SCRIPT_DIR}/07-ingress.yaml"

echo
echo "============================================================"
echo "10. Ожидаем готовность Deployment"
echo "============================================================"

if ! kubectl rollout status \
  deployment/order-service \
  --timeout=180s; then

  echo "Deployment не смог успешно запуститься."
  echo
  kubectl get pods
  echo
  kubectl describe deployment order-service
  exit 1
fi

echo
echo "============================================================"
echo "Установка успешно завершена"
echo "============================================================"

echo
echo "Pods:"
kubectl get pods

echo
echo "Jobs:"
kubectl get jobs

echo
echo "Services:"
kubectl get services

echo
echo "Ingress:"
kubectl get ingress