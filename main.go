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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultServerPort      = "8000"
	databaseRequestTimeout = 5 * time.Second
)

// HealthResponse описывает ответ проверки состояния сервиса.
type HealthResponse struct {
	Status string `json:"status"`
}

// User описывает пользователя, который хранится в PostgreSQL.
type User struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
}

// CreateUserRequest описывает запрос на создание пользователя.
type CreateUserRequest struct {
	Username  string `json:"username"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
}

// UpdateUserRequest описывает запрос на изменение пользователя.
// Указатели позволяют отличить отсутствующее поле от переданной пустой строки.
type UpdateUserRequest struct {
	Username  *string `json:"username"`
	FirstName *string `json:"firstName"`
	LastName  *string `json:"lastName"`
	Email     *string `json:"email"`
	Phone     *string `json:"phone"`
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
}

func main() {
	logger := log.New(os.Stdout, "", log.Ldate|log.Ltime|log.LUTC)

	db, err := connectDatabase(logger)
	if err != nil {
		logger.Fatalf("Не удалось подключиться к PostgreSQL: %v", err)
	}
	defer db.Close()

	app := &Application{
		db:     db,
		logger: logger,
	}

	mux := http.NewServeMux()

	// Сохраняем оба адреса health-check из предыдущего домашнего задания.
	mux.HandleFunc("GET /health", app.healthHandler)
	mux.HandleFunc("GET /health/{$}", app.healthHandler)
	registerMonitoringRoutes(mux, app)

	// RESTful CRUD пользователей.
	mux.HandleFunc("POST /user", app.createUserHandler)
	mux.HandleFunc("GET /user", app.getUsersHandler)
	mux.HandleFunc("GET /user/{userId}", app.getUserHandler)
	mux.HandleFunc("PUT /user/{userId}", app.updateUserHandler)
	mux.HandleFunc("DELETE /user/{userId}", app.deleteUserHandler)

	serverPort := getEnv("SERVER_PORT", defaultServerPort)
	server := &http.Server{
		Addr:              ":" + serverPort,
		Handler:           prometheusMiddleware(loggingMiddleware(logger, mux)),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)

	go func() {
		logger.Printf("Order service started on port :%s", serverPort)
		serverErrors <- server.ListenAndServe()
	}()

	shutdownSignals := make(chan os.Signal, 1)
	signal.Notify(shutdownSignals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case receivedSignal := <-shutdownSignals:
		logger.Printf("Получен сигнал остановки: %s", receivedSignal)
	case serverError := <-serverErrors:
		if !errors.Is(serverError, http.ErrServerClosed) {
			logger.Fatalf("HTTP-сервер завершился с ошибкой: %v", serverError)
		}
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownContext); err != nil {
		logger.Printf("Ошибка корректной остановки HTTP-сервера: %v", err)
	}

	logger.Println("Order service stopped")
}

// healthHandler проверяет работу приложения и доступность PostgreSQL.
func (app *Application) healthHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), databaseRequestTimeout)
	defer cancel()

	if err := app.db.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, ErrorResponse{
			Code:    http.StatusServiceUnavailable,
			Message: "database is unavailable",
		})
		return
	}

	writeJSON(w, http.StatusOK, HealthResponse{Status: "OK"})
}

// createUserHandler создаёт пользователя.
func (app *Application) createUserHandler(w http.ResponseWriter, r *http.Request) {
	var request CreateUserRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	user := User{
		Username:  strings.TrimSpace(request.Username),
		FirstName: strings.TrimSpace(request.FirstName),
		LastName:  strings.TrimSpace(request.LastName),
		Email:     strings.TrimSpace(request.Email),
		Phone:     strings.TrimSpace(request.Phone),
	}

	if err := validateUser(user); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), databaseRequestTimeout)
	defer cancel()

	query := `
		INSERT INTO users (username, first_name, last_name, email, phone)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`

	err := app.db.QueryRow(
		ctx,
		query,
		user.Username,
		user.FirstName,
		user.LastName,
		user.Email,
		user.Phone,
	).Scan(&user.ID)

	if err != nil {
		if isUniqueViolation(err) {
			writeError(w, http.StatusConflict, "user with this username or email already exists")
			return
		}

		app.logger.Printf("Ошибка создания пользователя: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to create user")
		return
	}

	writeJSON(w, http.StatusCreated, user)
}

// getUsersHandler возвращает всех пользователей.
func (app *Application) getUsersHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), databaseRequestTimeout)
	defer cancel()

	query := `
		SELECT id, username, first_name, last_name, email, phone
		FROM users
		ORDER BY id
	`

	rows, err := app.db.Query(ctx, query)
	if err != nil {
		app.logger.Printf("Ошибка получения списка пользователей: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to get users")
		return
	}
	defer rows.Close()

	users := make([]User, 0)

	for rows.Next() {
		var user User
		if err := rows.Scan(
			&user.ID,
			&user.Username,
			&user.FirstName,
			&user.LastName,
			&user.Email,
			&user.Phone,
		); err != nil {
			app.logger.Printf("Ошибка чтения пользователя: %v", err)
			writeError(w, http.StatusInternalServerError, "failed to read users")
			return
		}

		users = append(users, user)
	}

	if err := rows.Err(); err != nil {
		app.logger.Printf("Ошибка обработки списка пользователей: %v", err)
		writeError(w, http.StatusInternalServerError, "failed to process users")
		return
	}

	writeJSON(w, http.StatusOK, users)
}

// getUserHandler возвращает пользователя по ID.
func (app *Application) getUserHandler(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	user, err := app.getUserByID(r.Context(), userID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	if err != nil {
		app.logger.Printf("Ошибка получения пользователя ID %d: %v", userID, err)
		writeError(w, http.StatusInternalServerError, "failed to get user")
		return
	}

	writeJSON(w, http.StatusOK, user)
}

// updateUserHandler изменяет только поля, переданные в запросе.
func (app *Application) updateUserHandler(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var request UpdateUserRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if request.Username == nil &&
		request.FirstName == nil &&
		request.LastName == nil &&
		request.Email == nil &&
		request.Phone == nil {
		writeError(w, http.StatusBadRequest, "at least one user field must be provided")
		return
	}

	user, err := app.getUserByID(r.Context(), userID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}
	if err != nil {
		app.logger.Printf("Ошибка получения пользователя перед обновлением ID %d: %v", userID, err)
		writeError(w, http.StatusInternalServerError, "failed to get user")
		return
	}

	if request.Username != nil {
		user.Username = strings.TrimSpace(*request.Username)
	}
	if request.FirstName != nil {
		user.FirstName = strings.TrimSpace(*request.FirstName)
	}
	if request.LastName != nil {
		user.LastName = strings.TrimSpace(*request.LastName)
	}
	if request.Email != nil {
		user.Email = strings.TrimSpace(*request.Email)
	}
	if request.Phone != nil {
		user.Phone = strings.TrimSpace(*request.Phone)
	}

	if err := validateUser(user); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), databaseRequestTimeout)
	defer cancel()

	query := `
		UPDATE users
		SET username = $1,
			first_name = $2,
			last_name = $3,
			email = $4,
			phone = $5,
			updated_at = CURRENT_TIMESTAMP
		WHERE id = $6
	`

	_, err = app.db.Exec(
		ctx,
		query,
		user.Username,
		user.FirstName,
		user.LastName,
		user.Email,
		user.Phone,
		user.ID,
	)
	if err != nil {
		if isUniqueViolation(err) {
			writeError(w, http.StatusConflict, "user with this username or email already exists")
			return
		}

		app.logger.Printf("Ошибка обновления пользователя ID %d: %v", userID, err)
		writeError(w, http.StatusInternalServerError, "failed to update user")
		return
	}

	writeJSON(w, http.StatusOK, user)
}

// deleteUserHandler удаляет пользователя по ID.
func (app *Application) deleteUserHandler(w http.ResponseWriter, r *http.Request) {
	userID, err := parseUserID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), databaseRequestTimeout)
	defer cancel()

	commandTag, err := app.db.Exec(ctx, "DELETE FROM users WHERE id = $1", userID)
	if err != nil {
		app.logger.Printf("Ошибка удаления пользователя ID %d: %v", userID, err)
		writeError(w, http.StatusInternalServerError, "failed to delete user")
		return
	}

	if commandTag.RowsAffected() == 0 {
		writeError(w, http.StatusNotFound, "user not found")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// getUserByID получает пользователя из PostgreSQL.
func (app *Application) getUserByID(parentContext context.Context, userID int64) (User, error) {
	ctx, cancel := context.WithTimeout(parentContext, databaseRequestTimeout)
	defer cancel()

	query := `
		SELECT id, username, first_name, last_name, email, phone
		FROM users
		WHERE id = $1
	`

	var user User
	err := app.db.QueryRow(ctx, query, userID).Scan(
		&user.ID,
		&user.Username,
		&user.FirstName,
		&user.LastName,
		&user.Email,
		&user.Phone,
	)

	return user, err
}

// connectDatabase подключается к PostgreSQL с повторными попытками.
func connectDatabase(logger *log.Logger) (*pgxpool.Pool, error) {
	databaseURL := buildDatabaseURL()
	var lastError error

	for attempt := 1; attempt <= 30; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		pool, err := pgxpool.New(ctx, databaseURL)
		if err == nil {
			err = pool.Ping(ctx)
		}
		cancel()

		if err == nil {
			logger.Println("Подключение к PostgreSQL установлено")
			return pool, nil
		}

		if pool != nil {
			pool.Close()
		}

		lastError = err
		logger.Printf("PostgreSQL пока недоступен, попытка %d из 30: %v", attempt, err)
		time.Sleep(2 * time.Second)
	}

	return nil, lastError
}

// buildDatabaseURL формирует адрес PostgreSQL из переменных окружения.
func buildDatabaseURL() string {
	if databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL")); databaseURL != "" {
		return databaseURL
	}

	host := getEnv("DB_HOST", "localhost")
	port := getEnv("DB_PORT", "5432")
	name := getEnv("DB_NAME", "users")
	username := getEnv("DB_USERNAME", "order_user")
	password := getEnv("DB_PASSWORD", "order_password")
	sslMode := getEnv("DB_SSLMODE", "disable")

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

// validateUser проверяет обязательные поля пользователя.
func validateUser(user User) error {
	if user.Username == "" {
		return errors.New("username is required")
	}
	if len(user.Username) > 256 {
		return errors.New("username must not be longer than 256 characters")
	}
	if user.FirstName == "" {
		return errors.New("firstName is required")
	}
	if user.LastName == "" {
		return errors.New("lastName is required")
	}
	if user.Email == "" {
		return errors.New("email is required")
	}
	if !strings.Contains(user.Email, "@") {
		return errors.New("email has invalid format")
	}

	return nil
}

// decodeJSON читает одно JSON-значение и запрещает неизвестные поля.
func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}

	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain a single JSON object")
	}

	return nil
}

// parseUserID получает положительный ID из URL.
func parseUserID(r *http.Request) (int64, error) {
	value := r.PathValue("userId")
	userID, err := strconv.ParseInt(value, 10, 64)
	if err != nil || userID <= 0 {
		return 0, errors.New("userId must be a positive integer")
	}

	return userID, nil
}

// isUniqueViolation определяет нарушение UNIQUE-ограничения PostgreSQL.
func isUniqueViolation(err error) bool {
	var postgresError *pgconn.PgError
	return errors.As(err, &postgresError) && postgresError.Code == "23505"
}

// writeError отправляет ошибку в едином JSON-формате.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, ErrorResponse{
		Code:    status,
		Message: message,
	})
}

// writeJSON отправляет JSON-ответ.
func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if data == nil {
		return
	}

	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Ошибка формирования JSON-ответа: %v", err)
	}
}

// getEnv возвращает переменную окружения либо значение по умолчанию.
func getEnv(name, defaultValue string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return defaultValue
	}

	return value
}

// loggingMiddleware пишет краткий журнал HTTP-запросов.
func loggingMiddleware(logger *log.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		next.ServeHTTP(w, r)
		logger.Printf("%s %s выполнен за %s", r.Method, r.URL.Path, time.Since(startedAt))
	})
}
