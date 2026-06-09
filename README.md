# Order Service

Минимальный сервис заказов для учебного проекта BLT.

## Health check

```http
GET /health/
```

Ответ:

```json
{"status":"OK"}
```

## Сборка Docker-образа

```bash
docker build --platform linux/amd64 -t order-service:1.0.0 .
```

## Запуск контейнера

```bash
docker run --rm -p 8000:8000 order-service:1.0.0
```

## Проверка

```bash
curl http://localhost:8000/health/
```