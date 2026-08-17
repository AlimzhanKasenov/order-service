package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultServerPort = "8000"

	defaultJWTIssuer = "order-service"

	defaultJWTTTL = time.Hour

	minimumJWTSecretLength = 32

	databaseRequestTimeout = 5 * time.Second
)

// HealthResponse описывает ответ проверки состояния сервиса.
type HealthResponse struct {
	Status string `json:"status"`
}

// User описывает публичные данные профиля пользователя.
//
// password_hash намеренно отсутствует,
// поэтому хеш пароля никогда не попадёт в JSON-ответ.
type User struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
}

// ErrorResponse описывает единый формат ошибок API.
type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Application содержит общие зависимости HTTP-обработчиков.
type Application struct {
	db     *pgxpool.Pool
	logger *log.Logger

	jwtSecret []byte
	jwtIssuer string
	jwtTTL    time.Duration
}

func main() {
	logger := newJSONLogger()

	jwtSecret, jwtIssuer, jwtTTL, err :=
		loadJWTConfiguration()

	if err != nil {
		logger.Fatalf(
			"Некорректная конфигурация JWT: %v",
			err,
		)
	}

	db, err := connectDatabase(logger)
	if err != nil {
		logger.Fatalf(
			"Не удалось подключиться к PostgreSQL: %v",
			err,
		)
	}
	defer db.Close()

	app := &Application{
		db:        db,
		logger:    logger,
		jwtSecret: jwtSecret,
		jwtIssuer: jwtIssuer,
		jwtTTL:    jwtTTL,
	}

	kafkaContext, cancelKafka := context.WithCancel(
		context.Background(),
	)
	defer cancelKafka()

	go app.consumePaymentSucceededEvents(
		kafkaContext,
	)

	go app.consumePaymentFailedEvents(
		kafkaContext,
	)

	mux := http.NewServeMux()

	// Health-check из предыдущих домашних заданий.
	mux.HandleFunc(
		"GET /health",
		app.healthHandler,
	)

	mux.HandleFunc(
		"GET /health/{$}",
		app.healthHandler,
	)

	// Метрики Prometheus и тестовые monitoring-endpoints.
	registerMonitoringRoutes(
		mux,
		app,
	)

	// Публичные endpoints аутентификации.
	mux.HandleFunc(
		"POST /register",
		app.registerHandler,
	)

	mux.HandleFunc(
		"POST /login",
		app.loginHandler,
	)

	// Создание заказа.
	mux.HandleFunc(
		"POST /orders",
		app.createOrderHandler,
	)

	// Получение заказа.
	mux.HandleFunc(
		"GET /orders/{orderId}",
		app.getOrderHandler,
	)

	// Защищённое чтение собственного профиля.
	mux.Handle(
		"GET /profile/{userId}",
		app.profileAuthorizationMiddleware(
			http.HandlerFunc(
				app.getProfileHandler,
			),
		),
	)

	// Защищённое изменение собственного профиля.
	mux.Handle(
		"PUT /profile/{userId}",
		app.profileAuthorizationMiddleware(
			http.HandlerFunc(
				app.updateProfileHandler,
			),
		),
	)

	serverPort := getEnv(
		"SERVER_PORT",
		defaultServerPort,
	)

	server := &http.Server{
		Addr: ":" + serverPort,

		Handler: prometheusMiddleware(
			loggingMiddleware(
				logger,
				mux,
			),
		),

		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(
		chan error,
		1,
	)

	go func() {
		emitStructuredLog(
			"INFO",
			"Приложение запущено",
			map[string]any{
				"event":      "application_started",
				"port":       serverPort,
				"jwt_issuer": jwtIssuer,
				"jwt_ttl":    jwtTTL.String(),
			},
		)

		serverErrors <- server.ListenAndServe()
	}()

	shutdownSignals := make(
		chan os.Signal,
		1,
	)

	signal.Notify(
		shutdownSignals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	select {
	case receivedSignal := <-shutdownSignals:
		logger.Printf(
			"Получен сигнал остановки: %s",
			receivedSignal,
		)

	case serverError := <-serverErrors:
		if !errors.Is(
			serverError,
			http.ErrServerClosed,
		) {
			logger.Fatalf(
				"HTTP-сервер завершился с ошибкой: %v",
				serverError,
			)
		}
	}

	shutdownContext, cancel :=
		context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
	defer cancel()

	if err := server.Shutdown(
		shutdownContext,
	); err != nil {
		logger.Printf(
			"Ошибка корректной остановки HTTP-сервера: %v",
			err,
		)
	}

	emitStructuredLog(
		"INFO",
		"Приложение остановлено",
		map[string]any{
			"event": "application_stopped",
		},
	)
}

// healthHandler проверяет работу приложения
// и доступность PostgreSQL.
func (app *Application) healthHandler(
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx, cancel := context.WithTimeout(
		r.Context(),
		databaseRequestTimeout,
	)
	defer cancel()

	if err := app.db.Ping(ctx); err != nil {
		writeJSON(
			w,
			http.StatusServiceUnavailable,
			ErrorResponse{
				Code: http.StatusServiceUnavailable,

				Message: "database is unavailable",
			},
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		HealthResponse{
			Status: "OK",
		},
	)
}

// connectDatabase подключается к PostgreSQL
// с повторными попытками.
func connectDatabase(
	logger *log.Logger,
) (*pgxpool.Pool, error) {
	databaseURL := buildDatabaseURL()

	var lastError error

	for attempt := 1; attempt <= 30; attempt++ {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)

		pool, err := pgxpool.New(
			ctx,
			databaseURL,
		)

		if err == nil {
			err = pool.Ping(ctx)
		}

		cancel()

		if err == nil {
			logger.Println(
				"Подключение к PostgreSQL установлено",
			)

			return pool, nil
		}

		if pool != nil {
			pool.Close()
		}

		lastError = err

		logger.Printf(
			"PostgreSQL пока недоступен, попытка %d из 30: %v",
			attempt,
			err,
		)

		time.Sleep(2 * time.Second)
	}

	return nil, lastError
}

// buildDatabaseURL формирует адрес PostgreSQL
// из переменных окружения.
func buildDatabaseURL() string {
	databaseURL := strings.TrimSpace(
		os.Getenv("DATABASE_URL"),
	)

	if databaseURL != "" {
		return databaseURL
	}

	host := getEnv(
		"DB_HOST",
		"localhost",
	)

	port := getEnv(
		"DB_PORT",
		"5432",
	)

	name := getEnv(
		"DB_NAME",
		"users",
	)

	username := getEnv(
		"DB_USERNAME",
		"order_user",
	)

	password := getEnv(
		"DB_PASSWORD",
		"order_password",
	)

	sslMode := getEnv(
		"DB_SSLMODE",
		"disable",
	)

	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		username,
		password,
		host,
		port,
		name,
		sslMode,
	)
}

// loadJWTConfiguration загружает настройки JWT.
//
// JWT_SECRET не имеет небезопасного значения по умолчанию.
// Приложение не запускается без секрета длиной минимум 32 байта.
func loadJWTConfiguration() (
	[]byte,
	string,
	time.Duration,
	error,
) {
	secretValue := strings.TrimSpace(
		os.Getenv("JWT_SECRET"),
	)

	if len([]byte(secretValue)) <
		minimumJWTSecretLength {
		return nil, "", 0, fmt.Errorf(
			"JWT_SECRET must contain at least %d bytes",
			minimumJWTSecretLength,
		)
	}

	issuer := getEnv(
		"JWT_ISSUER",
		defaultJWTIssuer,
	)

	if strings.TrimSpace(issuer) == "" {
		return nil, "", 0, errors.New(
			"JWT_ISSUER must not be empty",
		)
	}

	ttlValue := getEnv(
		"JWT_TTL",
		defaultJWTTTL.String(),
	)

	ttl, err := time.ParseDuration(ttlValue)
	if err != nil {
		return nil, "", 0, fmt.Errorf(
			"JWT_TTL has invalid duration format: %w",
			err,
		)
	}

	if ttl <= 0 {
		return nil, "", 0, errors.New(
			"JWT_TTL must be greater than zero",
		)
	}

	return []byte(secretValue), issuer, ttl, nil
}

// validateUser проверяет обязательные поля пользователя.
func validateUser(
	user User,
) error {
	if user.Username == "" {
		return errors.New(
			"username is required",
		)
	}

	if len(user.Username) > 256 {
		return errors.New(
			"username must not be longer than 256 characters",
		)
	}

	if user.FirstName == "" {
		return errors.New(
			"firstName is required",
		)
	}

	if len(user.FirstName) > 255 {
		return errors.New(
			"firstName must not be longer than 255 characters",
		)
	}

	if user.LastName == "" {
		return errors.New(
			"lastName is required",
		)
	}

	if len(user.LastName) > 255 {
		return errors.New(
			"lastName must not be longer than 255 characters",
		)
	}

	if user.Email == "" {
		return errors.New(
			"email is required",
		)
	}

	if len(user.Email) > 255 {
		return errors.New(
			"email must not be longer than 255 characters",
		)
	}

	if !strings.Contains(
		user.Email,
		"@",
	) {
		return errors.New(
			"email has invalid format",
		)
	}

	if len(user.Phone) > 50 {
		return errors.New(
			"phone must not be longer than 50 characters",
		)
	}

	return nil
}

// decodeJSON читает одно JSON-значение
// и запрещает неизвестные поля.
func decodeJSON(
	w http.ResponseWriter,
	r *http.Request,
	destination any,
) error {
	r.Body = http.MaxBytesReader(
		w,
		r.Body,
		1<<20,
	)

	decoder := json.NewDecoder(r.Body)

	decoder.DisallowUnknownFields()

	if err := decoder.Decode(
		destination,
	); err != nil {
		return fmt.Errorf(
			"invalid JSON body: %w",
			err,
		)
	}

	if err := decoder.Decode(
		&struct{}{},
	); !errors.Is(err, io.EOF) {
		return errors.New(
			"request body must contain a single JSON object",
		)
	}

	return nil
}

// parseUserID получает положительный ID из URL.
func parseUserID(
	r *http.Request,
) (int64, error) {
	value := r.PathValue("userId")

	userID, err := strconv.ParseInt(
		value,
		10,
		64,
	)

	if err != nil || userID <= 0 {
		return 0, errors.New(
			"userId must be a positive integer",
		)
	}

	return userID, nil
}

// isUniqueViolation определяет нарушение
// UNIQUE-ограничения PostgreSQL.
func isUniqueViolation(
	err error,
) bool {
	var postgresError *pgconn.PgError

	return errors.As(
		err,
		&postgresError,
	) && postgresError.Code == "23505"
}

// writeError отправляет ошибку
// в едином JSON-формате.
func writeError(
	w http.ResponseWriter,
	status int,
	message string,
) {
	writeJSON(
		w,
		status,
		ErrorResponse{
			Code:    status,
			Message: message,
		},
	)
}

// writeJSON отправляет JSON-ответ.
func writeJSON(
	w http.ResponseWriter,
	status int,
	data any,
) {
	w.Header().Set(
		"Content-Type",
		"application/json; charset=utf-8",
	)

	w.Header().Set(
		"X-Content-Type-Options",
		"nosniff",
	)

	w.WriteHeader(status)

	if data == nil {
		return
	}

	if err := json.NewEncoder(w).Encode(
		data,
	); err != nil {
		log.Printf(
			"Ошибка формирования JSON-ответа: %v",
			err,
		)
	}
}

// getEnv возвращает переменную окружения
// либо значение по умолчанию.
func getEnv(
	name string,
	defaultValue string,
) string {
	value := strings.TrimSpace(
		os.Getenv(name),
	)

	if value == "" {
		return defaultValue
	}

	return value
}

// loggingMiddleware пишет журнал HTTP-запросов.
func loggingMiddleware(
	logger *log.Logger,
	next http.Handler,
) http.Handler {
	return structuredLoggingMiddleware(
		logger,
		next,
	)
}
