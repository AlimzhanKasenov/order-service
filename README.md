# Order Service

Учебный проект на Go с PostgreSQL, Apache Kafka и развёртыванием в Kubernetes.

Проект последовательно развивается в рамках домашних заданий по
микросервисной архитектуре и включает:

- REST API;
- PostgreSQL;
- Docker;
- Kubernetes;
- NGINX Ingress;
- Apache Kafka;
- событийное взаимодействие микросервисов;
- Prometheus и Grafana;
- централизованное логирование через EFK;
- регистрацию и аутентификацию пользователей;
- JWT-авторизацию;
- безопасную работу с пользовательским профилем.

---

# Домашнее задание: Stream Processing

В рамках домашнего задания Stream Processing реализовано взаимодействие
трёх микросервисов:

- **Order Service** — пользователи и заказы;
- **Billing Service** — счета пользователей и операции с балансом;
- **Notification Service** — уведомления о результате оплаты.

Для событийного взаимодействия используется **Apache Kafka**.

В практической части реализован архитектурный стиль
**Event Collaboration**.

## Теоретическая часть

Полная теоретическая часть домашнего задания находится в:

**[docs/theory.md](docs/theory.md)**

В документе рассмотрены все требуемые архитектурные варианты:

1. только HTTP-взаимодействие;
2. HTTP-взаимодействие с использованием брокера сообщений только
   для уведомлений;
3. Event Collaboration с использованием брокера сообщений;
4. наиболее подходящий вариант для данной задачи.

Для каждого варианта приведены:

- sequence-диаграмма;
- описание взаимодействия сервисов;
- преимущества;
- недостатки.

В качестве наиболее подходящего варианта выбран
**Event Collaboration**.

Он совпадает с вариантом №3.

## Sequence-диаграммы

Sequence-диаграммы всех архитектурных вариантов расположены в:

**[docs/theory.md](docs/theory.md)**

В документе представлены:

```text
Вариант 1:
только HTTP

Вариант 2:
HTTP + Kafka только для Notification Service

Вариант 3:
Event Collaboration

Вариант 4:
выбранная архитектура
```

Также отдельно показан процесс автоматического создания billing account
после регистрации пользователя.

## Схема работы приложения

Общая архитектурная схема приложения также находится в:

**[docs/theory.md](docs/theory.md)**

Упрощённая схема реализованного взаимодействия:

```text
                         HTTP
                  Client / Postman
                         |
                         v
                   NGINX Ingress
                  arch.homework
                         |
          +--------------+--------------+
          |              |              |
          v              v              v
   Order Service   Billing Service   Notification
      :8000            :8001        Service :8002
          |              |              |
          |              |              |
          v              v              v
     PostgreSQL     PostgreSQL      PostgreSQL
    Users/Orders     Accounts      Notifications

             Order Service
                  |
                  | user.created
                  | order.created
                  v
                Kafka
               :29092
                  |
          +-------+-------+
          |               |
          v               v
   Billing Service      other
                       consumers

Billing Service
      |
      | payment.succeeded
      | payment.failed
      v
    Kafka
    /   \
   /     \
  v       v
Order   Notification
Service   Service
```

## Выбранный подход

Для практической реализации выбран **Event Collaboration**.

В этом варианте сервисы не вызывают друг друга напрямую для выполнения
всего бизнес-процесса.

Вместо этого они публикуют события в Kafka и самостоятельно реагируют
на интересующие их события.

Например:

```text
Order Service
    |
    | order.created
    v
Kafka
    |
    v
Billing Service
```

После выполнения оплаты:

```text
Billing Service
       |
       | payment.succeeded
       | или
       | payment.failed
       v
     Kafka
     /   \
    /     \
   v       v
Order   Notification
Service   Service
```

## Почему выбран Event Collaboration

Основное преимущество Event Collaboration —
слабая связанность микросервисов.

Order Service отвечает за пользователей и заказы.

Billing Service отвечает за счета и операции с балансом.

Notification Service отвечает за уведомления.

При этом Order Service не обязан знать о существовании
Notification Service.

После публикации события:

```text
payment.succeeded
```

его одновременно могут обрабатывать разные сервисы:

```text
Order Service
Notification Service
Analytics Service
Audit Service
Loyalty Service
```

Добавление нового consumer не требует изменения Billing Service.

### Преимущества

- слабая связанность сервисов;
- независимое масштабирование;
- возможность добавлять новых consumers;
- Notification Service не блокирует выполнение оплаты;
- Billing Service самостоятельно реагирует на новые заказы;
- Order Service самостоятельно получает результат оплаты;
- хорошая расширяемость архитектуры;
- Kafka позволяет независимо обрабатывать события разными сервисами.

### Недостатки

- архитектура сложнее синхронного HTTP;
- требуется отдельный брокер сообщений;
- появляется eventual consistency;
- сложнее трассировать весь бизнес-процесс;
- необходимо учитывать consumer groups и offsets;
- необходимо контролировать повторную обработку событий;
- для production желательно обеспечивать идемпотентность;
- для production желательно использовать Outbox Pattern.

Подробное сравнение всех вариантов:

**[docs/theory.md](docs/theory.md)**

---

## IDL

Для описания контрактов используются два IDL.

### OpenAPI

HTTP API описан в формате **OpenAPI 3**:

**[docs/openapi.yaml](docs/openapi.yaml)**

В OpenAPI описаны основные HTTP endpoints:

```text
POST /register

POST /orders
GET  /orders/{orderId}

GET  /billing/accounts/{userId}
POST /billing/accounts/{userId}/deposit
POST /billing/accounts/{userId}/withdraw

GET  /notifications/{userId}
POST /notifications
```

### AsyncAPI

Событийное взаимодействие через Kafka описано в формате **AsyncAPI**:

**[docs/asyncapi.yaml](docs/asyncapi.yaml)**

Описаны события:

```text
user.created
order.created
payment.succeeded
payment.failed
```

---

## Kafka events

### user.created

Producer:

```text
Order Service
```

Consumer:

```text
Billing Service
```

Используется для автоматического создания billing account после
регистрации пользователя.

Сценарий:

```text
POST /register
      |
      v
Order Service
      |
      | user.created
      v
    Kafka
      |
      v
Billing Service
      |
      v
Account(balance = 0)
```

---

### order.created

Producer:

```text
Order Service
```

Consumer:

```text
Billing Service
```

Событие запускает процесс оплаты нового заказа.

```text
POST /orders
      |
      v
Order Service
      |
      | order.created
      v
    Kafka
      |
      v
Billing Service
```

---

### payment.succeeded

Producer:

```text
Billing Service
```

Consumers:

```text
Order Service
Notification Service
```

После события:

```text
Order Service:
NEW -> PAID

Notification Service:
создаёт SUCCESS notification
```

---

### payment.failed

Producer:

```text
Billing Service
```

Consumers:

```text
Order Service
Notification Service
```

После события:

```text
Order Service:
NEW -> PAYMENT_FAILED

Notification Service:
создаёт FAILED notification
```

При недостаточном количестве средств баланс пользователя не изменяется.

---

## Основной сценарий Stream Processing

Реализован следующий сценарий.

### 1. Регистрация пользователя

Клиент отправляет:

```http
POST /register
```

Order Service создаёт пользователя и публикует:

```text
user.created
```

Billing Service получает событие и автоматически создаёт счёт:

```text
balance = 0
```

### 2. Пополнение счёта

Клиент вызывает:

```http
POST /billing/accounts/{userId}/deposit
```

Например:

```json
{
  "amount": 10000
}
```

После этого:

```text
balance = 10000
```

### 3. Создание успешного заказа

Создаётся заказ:

```json
{
  "userId": 1,
  "price": 3000
}
```

Order Service сохраняет заказ:

```text
status = NEW
```

и публикует:

```text
order.created
```

Billing Service получает событие и списывает:

```text
3000
```

После списания баланс:

```text
7000
```

Billing Service публикует:

```text
payment.succeeded
```

Order Service получает событие:

```text
NEW -> PAID
```

Notification Service создаёт уведомление:

```text
SUCCESS
```

### 4. Создание заказа при недостаточном балансе

Создаётся заказ:

```json
{
  "userId": 1,
  "price": 20000
}
```

Billing Service проверяет баланс:

```text
7000 < 20000
```

Деньги не списываются.

Баланс остаётся:

```text
7000
```

Billing Service публикует:

```text
payment.failed
```

Order Service изменяет статус:

```text
NEW -> PAYMENT_FAILED
```

Notification Service создаёт:

```text
FAILED
```

---

## Проверенный результат Stream Processing

Функциональный сценарий был проверен в Kubernetes.

После регистрации пользователя:

```text
userId = 1
billing balance = 0
```

После пополнения:

```text
balance = 10000
```

После заказа стоимостью:

```text
3000
```

получен результат:

```text
order status = PAID
balance = 7000
notification = SUCCESS
```

После заказа стоимостью:

```text
20000
```

получен результат:

```text
order status = PAYMENT_FAILED
balance = 7000
notification = FAILED
```

Таким образом проверены оба обязательных сценария:

```text
успешная оплата
недостаточно средств
```

---

# Stream Processing — Kubernetes

Микросервисы Stream Processing разворачиваются в отдельном namespace:

```text
stream-processing
```

Kubernetes-манифесты находятся в:

```text
k8s/stream-processing/
```

Структура:

```text
k8s/stream-processing/
├── 00-namespace.yaml
├── 01-secrets.yaml
├── 02-order-postgres.yaml
├── 03-billing-postgres.yaml
├── 04-notification-postgres.yaml
├── 05-kafka.yaml
├── 06-order-service.yaml
├── 07-billing-service.yaml
├── 08-notification-service.yaml
├── 09-ingress.yaml
└── apply.sh
```

## Установка Stream Processing

Перед установкой необходимо запустить Kubernetes и включить
NGINX Ingress.

Например, для Minikube:

```bash
minikube start \
  -p stream-processing \
  --driver=docker \
  --cpus=4 \
  --memory=5120
```

Переключить context:

```bash
minikube update-context \
  -p stream-processing
```

Включить Ingress:

```bash
minikube addons enable ingress \
  -p stream-processing
```

Установка приложения:

```bash
./k8s/stream-processing/apply.sh
```

Скрипт:

1. создаёт namespace;
2. создаёт Secret;
3. запускает PostgreSQL;
4. ожидает готовности PostgreSQL;
5. запускает Kafka;
6. ожидает готовности Kafka;
7. запускает микросервисы;
8. ожидает их готовности;
9. создаёт Ingress.

Проверка:

```bash
kubectl get pods \
  -n stream-processing
```

Ожидаемые компоненты:

```text
order-postgres
billing-postgres
notification-postgres
kafka
order-service
billing-service
notification-service
```

Все pod должны иметь состояние:

```text
1/1 Running
```

---

## Docker images Stream Processing

Используются следующие Docker images:

```text
alimzhankassenov/order-service:stream-processing
alimzhankassenov/billing-service:1.0.0
alimzhankassenov/notification-service:1.0.0
apache/kafka:4.1.2
postgres:17-alpine
```

---

## Stream Processing Ingress

Основной host:

```text
arch.homework
```

Маршрутизация:

```text
/                        -> Order Service
/billing                 -> Billing Service
/notifications           -> Notification Service
```

В окружении WSL2 + Docker Desktop прямое обращение к IP Minikube
может быть недоступно.

Для локальной проверки можно использовать:

```bash
kubectl port-forward \
  -n ingress-nginx \
  service/ingress-nginx-controller \
  18080:80
```

После этого запрос:

```bash
curl \
  -H 'Host: arch.homework' \
  http://127.0.0.1:18080/health
```

Ожидаемый ответ:

```json
{
  "status": "OK"
}
```

---

# Предыдущие домашние задания

Ниже сохранено описание функциональности, реализованной в предыдущих
этапах проекта.

## Возможности

- регистрация пользователей;
- вход по логину и паролю;
- хранение паролей в виде BCrypt-хеша;
- выдача JWT после успешного входа;
- проверка подписи и срока действия JWT;
- просмотр собственного профиля;
- изменение собственного профиля;
- запрет доступа к профилю другого пользователя;
- health-check приложения и PostgreSQL;
- Prometheus metrics;
- конфигурация приложения через ConfigMap;
- хранение доступов и JWT secret через Kubernetes Secret;
- миграции PostgreSQL;
- доступ через NGINX Ingress;
- централизованное логирование через Elasticsearch, Fluent Bit и Kibana;
- мониторинг через Prometheus и Grafana.

---

## Архитектура Authentication / BFF

Схема предыдущего домашнего задания Authentication/BFF:

![Authentication and BFF](docs/auth-bff-architecture.svg)

Основной маршрут запроса:

```text
Postman / Newman
        |
        v
NGINX Ingress
   API Gateway
        |
        v
Go Order Service / BFF
        |
        +-- Registration
        +-- Login
        +-- BCrypt
        +-- JWT
        +-- Authorization
        +-- Profile
        |
        v
PostgreSQL
```

NGINX Ingress используется в качестве единой точки входа и API Gateway.

Go-приложение выполняет роль BFF для сценария регистрации,
аутентификации и работы пользователя со своим профилем.

---

## API Authentication / BFF

| Метод | URL | Доступ | Описание |
|---|---|---|---|
| GET | `/health` | публичный | Проверка состояния приложения и PostgreSQL |
| GET | `/health/` | публичный | Проверка состояния приложения и PostgreSQL |
| GET | `/metrics` | публичный | Метрики Prometheus |
| POST | `/register` | публичный | Регистрация пользователя |
| POST | `/login` | публичный | Вход и получение JWT |
| GET | `/profile/{userId}` | Bearer JWT | Получение собственного профиля |
| PUT | `/profile/{userId}` | Bearer JWT | Изменение собственного профиля |

---

## Аутентификация

При регистрации пароль пользователя не сохраняется в открытом виде.

Приложение создаёт BCrypt-хеш и сохраняет его в PostgreSQL:

```text
password_hash
```

Пример регистрации:

```http
POST /register
Content-Type: application/json
```

```json
{
  "username": "user1",
  "password": "Password123!",
  "firstName": "User",
  "lastName": "One",
  "email": "user1@example.com",
  "phone": "+77010000001"
}
```

После успешной аутентификации:

```http
POST /login
```

сервер возвращает JWT:

```json
{
  "tokenType": "Bearer",
  "accessToken": "JWT_TOKEN",
  "expiresIn": 3600,
  "userId": 1
}
```

JWT подписывается алгоритмом HS256.

Срок действия access token:

```text
1 час
```

---

## Авторизация профиля

ID аутентифицированного пользователя хранится в стандартном JWT claim:

```text
sub
```

Для каждого запроса профиля приложение сравнивает:

```text
JWT sub == userId из URL
```

Возможные результаты:

```text
JWT отсутствует           -> 401 Unauthorized
JWT недействителен        -> 401 Unauthorized
пользователь читает себя  -> 200 OK
пользователь меняет себя  -> 200 OK
доступ к чужому профилю   -> 403 Forbidden
```

Таким образом аутентифицированный пользователь не может читать
или изменять профиль другого клиента.

---

## Logout

Отдельный endpoint logout не реализован.

Приложение использует stateless JWT.

Для выхода клиент удаляет access token.

После окончания срока действия JWT сервер также перестаёт принимать
токен.

---

# Локальный запуск

Запустить PostgreSQL, Kafka и микросервисы:

```bash
docker compose up --build -d
```

Проверить контейнеры:

```bash
docker compose ps
```

Health-check Order Service:

```bash
curl http://localhost:8000/health
```

Billing Service:

```bash
curl http://localhost:8001/health
```

Notification Service:

```bash
curl http://localhost:8002/health
```

Ожидаемый ответ health-check:

```json
{
  "status": "OK"
}
```

Остановить приложение:

```bash
docker compose down
```

Для полного удаления локальных PostgreSQL volumes:

```bash
docker compose down -v
```

---

# Docker

## Order Service

Пример сборки:

```bash
docker build \
  -t order-service:4.0.0 \
  .
```

Stream Processing image:

```text
alimzhankassenov/order-service:stream-processing
```

## Billing Service

Исходный код:

```text
services/billing-service/
```

Docker image:

```text
alimzhankassenov/billing-service:1.0.0
```

## Notification Service

Исходный код:

```text
services/notification-service/
```

Docker image:

```text
alimzhankassenov/notification-service:1.0.0
```

---

# Миграции

Для Order Service используются:

```text
migrations/001_create_users.sql
migrations/002_add_authentication.sql
migrations/003_create_orders.sql
```

Миграция:

```text
002_add_authentication.sql
```

добавляет поле:

```text
password_hash
```

Миграция:

```text
003_create_orders.sql
```

создаёт таблицу:

```text
orders
```

Billing Service:

```text
services/billing-service/migrations/001_create_accounts.sql
```

Notification Service:

```text
services/notification-service/migrations/001_create_notifications.sql
```

---

# Kubernetes предыдущего этапа

Для предыдущих домашних заданий использовался основной каталог:

```text
k8s/
```

Приложение устанавливалось в namespace:

```text
default
```

Использовался Minikube.

Запуск:

```bash
minikube start
```

Проверка:

```bash
minikube status
kubectl get nodes
```

Включение NGINX Ingress Controller:

```bash
minikube addons enable ingress
```

---

## PostgreSQL предыдущего этапа

PostgreSQL устанавливался через Helm.

Helm release:

```text
order-db
```

Service PostgreSQL:

```text
order-postgresql
```

Настройки Helm:

```text
k8s/postgres-values.yaml
```

---

## Развёртывание предыдущего Order Service

Применение Secret и ConfigMap:

```bash
kubectl apply \
  -f k8s/01-secret.yaml \
  -f k8s/02-configmap.yaml \
  -f k8s/03-migration-configmap.yaml
```

Перезапуск миграции:

```bash
kubectl delete job \
  order-service-migration \
  --ignore-not-found

kubectl apply \
  -f k8s/04-migration-job.yaml
```

Установка приложения:

```bash
kubectl apply \
  -f k8s/00-ingress-class.yaml \
  -f k8s/05-deployment.yaml \
  -f k8s/06-service.yaml \
  -f k8s/07-ingress.yaml
```

Проверка:

```bash
kubectl get pods
kubectl get jobs
kubectl get svc
kubectl get ingress
```

---

# Postman — Authentication / BFF

Коллекция предыдущего домашнего задания:

```text
postman/auth-bff.postman_collection.json
```

В коллекции используется переменная:

```text
{{baseUrl}}
```

Initial value:

```text
http://arch.homework
```

При каждом запуске автоматически генерируются случайные:

- username пользователя 1;
- password пользователя 1;
- email пользователя 1;
- username пользователя 2;
- password пользователя 2;
- email пользователя 2.

---

# Newman — Authentication / BFF

Для запуска тестов предыдущего домашнего задания:

```bash
./scripts/run-auth-bff-newman.sh
```

Скрипт использует Ingress через:

```text
http://arch.homework:8080
```

При этом initial value переменной `{{baseUrl}}`
в самой Postman-коллекции остаётся:

```text
http://arch.homework
```

Во время запуска Newman выводит в командную строку:

- HTTP-метод;
- URL;
- данные запроса;
- HTTP-код ответа;
- данные ответа.

Тестовый сценарий Authentication/BFF:

1. регистрация пользователя 1;
2. проверка запрета чтения профиля без логина;
3. проверка запрета изменения профиля без логина;
4. вход пользователя 1;
5. изменение профиля пользователя 1;
6. проверка сохранённых изменений;
7. регистрация пользователя 2;
8. вход пользователя 2;
9. проверка запрета чтения профиля пользователя 1 пользователем 2;
10. проверка запрета изменения профиля пользователя 1 пользователем 2.

Успешный результат:

```text
requests:   10
failed:      0

assertions: 17
failed:      0
```

---

# Централизованное логирование

В проекте настроен EFK-стек:

- Elasticsearch;
- Kibana;
- Fluent Bit.

Подробная инструкция:

[Централизованное логирование](LOGGING.md)

Скриншоты Kibana находятся в:

```text
screenshots/kibana/
```

---

# Мониторинг

Для мониторинга используются:

- Prometheus;
- Grafana;
- ServiceMonitor;
- PostgreSQL Exporter;
- метрики NGINX Ingress.

Grafana dashboard:

```text
grafana/order-service-monitoring-dashboard.json
```

Alert rules:

```text
grafana/order-service-alert-rules.json
```

---

# Документация

## Stream Processing

Теоретическая часть:

**[docs/theory.md](docs/theory.md)**

OpenAPI:

**[docs/openapi.yaml](docs/openapi.yaml)**

AsyncAPI:

**[docs/asyncapi.yaml](docs/asyncapi.yaml)**

Kubernetes:

```text
k8s/stream-processing/
```

## Authentication / BFF

Архитектурная схема:

**[docs/auth-bff-architecture.svg](docs/auth-bff-architecture.svg)**

Подробное описание:

```text
AUTHENTICATION.md
```

## Logging

```text
LOGGING.md
```
