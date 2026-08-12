# Аутентификация и BFF

Домашнее задание по реализации безопасного доступа
к пользовательскому профилю.

## Архитектура

![Authentication and BFF](docs/auth-bff-architecture.svg)

Схема взаимодействия:

```text
Postman / Newman
        |
        | HTTP
        v
NGINX Ingress
API Gateway
        |
        v
Go order-service / BFF
        |
        +-- Регистрация
        +-- BCrypt
        +-- Login
        +-- JWT
        +-- Авторизация
        +-- Profile
        |
        v
PostgreSQL
```

NGINX Ingress является единой точкой входа
и выполняет роль API Gateway.

Go-приложение выполняет роль BFF для сценария
работы пользователя со своим профилем.

## Аутентификация

Регистрация:

```text
POST /register
```

Пароль пользователя не хранится в открытом виде.

Перед сохранением пароль хешируется с помощью BCrypt.
В PostgreSQL сохраняется только `password_hash`.

Вход:

```text
POST /login
```

После успешной проверки username и password
приложение выдаёт JWT.

ID пользователя сохраняется в стандартном claim:

```text
sub
```

JWT подписывается алгоритмом HS256.

Срок действия токена:

```text
1 час
```

## Авторизация

Защищённые endpoints:

```text
GET /profile/{userId}
PUT /profile/{userId}
```

При обращении к профилю приложение сравнивает:

```text
JWT sub == userId из URL
```

Возможные результаты:

```text
без JWT                 -> 401 Unauthorized
недействительный JWT    -> 401 Unauthorized
свой профиль            -> 200 OK
чужой профиль           -> 403 Forbidden
```

Таким образом один аутентифицированный пользователь
не может читать или изменять профиль другого пользователя.

## API

| Метод | URL | Описание |
|---|---|---|
| POST | `/register` | Регистрация |
| POST | `/login` | Вход и получение JWT |
| GET | `/profile/{userId}` | Получение своего профиля |
| PUT | `/profile/{userId}` | Изменение своего профиля |
| GET | `/health` | Health-check |
| GET | `/metrics` | Prometheus metrics |

## Logout

Отдельный logout endpoint не реализован.

Используется stateless JWT.

Для выхода клиент удаляет access token.
После истечения срока действия JWT сервер
также перестаёт принимать этот токен.

## Kubernetes namespace

Приложение устанавливается в namespace:

```text
default
```

## API Gateway

Используется NGINX Ingress Controller.

В Minikube он устанавливается командой:

```bash
minikube addons enable ingress
```

Отдельный API Gateway не требуется.

## Docker-образ

Сборка:

```bash
docker build \
  -t order-service:4.0.0 \
  .
```

Загрузка образа в Minikube:

```bash
minikube image load order-service:4.0.0
```

## Установка

Применение Secret и ConfigMap:

```bash
kubectl apply \
  -f k8s/01-secret.yaml \
  -f k8s/02-configmap.yaml \
  -f k8s/03-migration-configmap.yaml
```

Запуск миграции:

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

Deployment запускает две реплики `order-service`.

## Домен

Initial URL приложения:

```text
http://arch.homework
```

В Postman используется переменная:

```text
{{baseUrl}}
```

Initial value:

```text
http://arch.homework
```

В текущем окружении WSL2 + Docker driver
Minikube IP недоступен напрямую из WSL.

Для локального запуска тестов Ingress пробрасывается:

```bash
kubectl port-forward \
  -n ingress-nginx \
  service/ingress-nginx-controller \
  8080:80
```

В `/etc/hosts`:

```text
127.0.0.1 arch.homework
```

После этого доступен:

```text
http://arch.homework:8080
```

## Postman / Newman

Коллекция:

```text
postman/auth-bff.postman_collection.json
```

Тестовые данные генерируются случайно
при каждом запуске коллекции.

Проверяется сценарий:

1. регистрация пользователя 1;
2. чтение профиля без входа — `401`;
3. изменение профиля без входа — `401`;
4. вход пользователя 1;
5. изменение профиля пользователя 1;
6. проверка изменённых данных;
7. регистрация пользователя 2;
8. вход пользователя 2;
9. пользователь 2 не может прочитать профиль пользователя 1 — `403`;
10. пользователь 2 не может изменить профиль пользователя 1 — `403`.

Запуск:

```bash
./scripts/run-auth-bff-newman.sh
```

Результат:

```text
newman/auth-bff-newman-result.txt
```

При запуске Newman выводит в командную строку:

- URL запроса;
- метод запроса;
- body запроса;
- HTTP status;
- body ответа.

Успешный результат:

```text
requests:   10
failed:      0

assertions: 17
failed:      0
```