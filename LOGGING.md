# Централизованное логирование сервисов

В проекте настроены структурированные JSON-логи Go CRUD-сервиса и EFK-стек:

- Elasticsearch
- Kibana
- Fluent Bit
- Kubernetes
- сбор логов migration Job

## Namespace

Приложение и PostgreSQL устанавливаются в namespace `default`.

EFK-стек устанавливается в namespace `logging`.

## Сборка приложения

```bash
minikube image build -t order-service:3.0.0 .
```

## Установка приложения

```bash
chmod +x k8s/apply.sh
./k8s/apply.sh
```

## Установка EFK

```bash
chmod +x k8s/logging/apply.sh
./k8s/logging/apply.sh
```

## Открытие Kibana

```bash
kubectl port-forward -n logging service/kibana-service 5601:5601
```

Открыть: `http://localhost:5601`

## Data View

- Name: `Order Service Logs`
- Index pattern: `fluent-bit-logs-*`
- Timestamp field: `@timestamp`

## Генерация тестовых логов

```bash
chmod +x scripts/generate-logs.sh
./scripts/generate-logs.sh
```

## KQL-запросы

Все логи приложения:

```text
service.keyword : "order-service"
```

WARN и ERROR:

```text
service.keyword : "order-service" and level.keyword : ("WARN" or "ERROR")
```

Migration Job:

```text
service.keyword : "order-service-migration"
```

## Скриншоты

Скриншоты Kibana находятся в файле:

`screenshots/kibana/scren.docx`
