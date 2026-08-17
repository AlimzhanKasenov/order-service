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

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultServerPort      = "8002"
	databaseRequestTimeout = 5 * time.Second
)

// Application содержит зависимости Notification Service.
type Application struct {
	db     *pgxpool.Pool
	logger *log.Logger
}

// HealthResponse описывает health-check.
type HealthResponse struct {
	Status string `json:"status"`
}

// ErrorResponse описывает ошибку API.
type ErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func main() {
	logger := log.New(
		os.Stdout,
		"",
		log.LstdFlags|log.LUTC,
	)

	db, err := connectDatabase(logger)
	if err != nil {
		logger.Fatalf(
			"Не удалось подключиться к PostgreSQL: %v",
			err,
		)
	}
	defer db.Close()

	app := &Application{
		db:     db,
		logger: logger,
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

	mux.HandleFunc(
		"GET /health",
		app.healthHandler,
	)

	mux.HandleFunc(
		"GET /health/{$}",
		app.healthHandler,
	)

	// Временный HTTP endpoint создания уведомления.
	//
	// Позже Notification Service будет создавать записи
	// автоматически из Kafka-событий payment.succeeded
	// и payment.failed.
	mux.HandleFunc(
		"POST /notifications",
		app.createNotificationHandler,
	)

	// Получение уведомлений конкретного пользователя.
	mux.HandleFunc(
		"GET /notifications/{userId}",
		app.getNotificationsHandler,
	)

	serverPort := getEnv(
		"SERVER_PORT",
		defaultServerPort,
	)

	server := &http.Server{
		Addr: ":" + serverPort,

		Handler: loggingMiddleware(
			logger,
			mux,
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
		logger.Printf(
			"Notification Service запущен на порту %s",
			serverPort,
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

	shutdownContext, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(shutdownContext); err != nil {
		logger.Printf(
			"Ошибка корректной остановки сервиса: %v",
			err,
		)
	}

	logger.Println(
		"Notification Service остановлен",
	)
}

// healthHandler проверяет сервис и PostgreSQL.
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
		writeError(
			w,
			http.StatusServiceUnavailable,
			"database is unavailable",
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

// connectDatabase подключается к PostgreSQL.
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

// buildDatabaseURL формирует строку подключения.
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
		"5434",
	)

	name := getEnv(
		"DB_NAME",
		"notifications",
	)

	username := getEnv(
		"DB_USERNAME",
		"notification_user",
	)

	password := getEnv(
		"DB_PASSWORD",
		"notification_password",
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

// decodeJSON читает одно JSON-значение.
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

	if err := decoder.Decode(destination); err != nil {
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

// parseUserID читает userId из URL.
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

// writeError возвращает JSON-ошибку.
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

// writeJSON возвращает JSON.
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

	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf(
			"Ошибка формирования JSON-ответа: %v",
			err,
		)
	}
}

// getEnv возвращает переменную окружения
// или значение по умолчанию.
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

// loggingMiddleware пишет HTTP-логи.
func loggingMiddleware(
	logger *log.Logger,
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(
		func(
			w http.ResponseWriter,
			r *http.Request,
		) {
			start := time.Now()

			next.ServeHTTP(w, r)

			logger.Printf(
				"%s %s duration=%s",
				r.Method,
				r.URL.Path,
				time.Since(start),
			)
		},
	)
}
