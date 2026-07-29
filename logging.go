package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultServiceName = "order-service"

var structuredLogMutex sync.Mutex

// newJSONLogger создаёт совместимый со стандартным log.Logger JSON-логгер.
// Благодаря этому существующий код приложения продолжает работать,
// но каждая строка выводится в stdout как отдельный JSON-документ.
func newJSONLogger() *log.Logger {
	return log.New(&structuredLogWriter{}, "", 0)
}

// structuredLogWriter преобразует обычные сообщения log.Logger в JSON.
type structuredLogWriter struct{}

func (writer *structuredLogWriter) Write(data []byte) (int, error) {
	message := strings.TrimSpace(string(data))
	if message == "" {
		return len(data), nil
	}

	emitStructuredLog(classifyLogLevel(message), message, nil)

	return len(data), nil
}

// classifyLogLevel определяет уровень для сообщений существующего кода.
func classifyLogLevel(message string) string {
	normalizedMessage := strings.ToLower(message)

	switch {
	case strings.Contains(normalizedMessage, "ошиб"),
		strings.Contains(normalizedMessage, "не удалось"),
		strings.Contains(normalizedMessage, "failed"),
		strings.Contains(normalizedMessage, "fatal"):
		return "ERROR"

	case strings.Contains(normalizedMessage, "недоступ"),
		strings.Contains(normalizedMessage, "повтор"),
		strings.Contains(normalizedMessage, "retry"):
		return "WARN"

	default:
		return "INFO"
	}
}

// structuredLoggingMiddleware пишет HTTP-, CRUD-, WARN- и ERROR-события.
func structuredLoggingMiddleware(
	_ *log.Logger,
	next http.Handler,
) http.Handler {
	return http.HandlerFunc(func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		// Запросы Prometheus не включаем в прикладные логи,
		// иначе scrape будет создавать лишний шум в Kibana.
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		startedAt := time.Now()

		recorder := &loggingResponseWriter{
			ResponseWriter: w,
		}

		next.ServeHTTP(recorder, r)

		statusCode := recorder.statusCode
		if statusCode == 0 {
			statusCode = http.StatusOK
		}

		baseFields := map[string]any{
			"event":       "http_request",
			"method":      r.Method,
			"path":        r.URL.Path,
			"query":       r.URL.RawQuery,
			"ip":          clientIP(r),
			"status":      statusCode,
			"duration_ms": time.Since(startedAt).Milliseconds(),
		}

		emitStructuredLog(
			"INFO",
			"HTTP-запрос обработан",
			baseFields,
		)

		emitRequestResultLog(
			r,
			recorder,
			statusCode,
		)
	})
}

// loggingResponseWriter сохраняет HTTP-код и небольшое тело ответа.
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
	body       bytes.Buffer
}

// WriteHeader сохраняет первый HTTP-код ответа.
func (recorder *loggingResponseWriter) WriteHeader(
	statusCode int,
) {
	if recorder.statusCode != 0 {
		return
	}

	recorder.statusCode = statusCode
	recorder.ResponseWriter.WriteHeader(statusCode)
}

// Write сохраняет часть ответа для определения user_id или текста ошибки.
func (recorder *loggingResponseWriter) Write(
	data []byte,
) (int, error) {
	if recorder.statusCode == 0 {
		recorder.WriteHeader(http.StatusOK)
	}

	// Для определения user_id и текста ошибки
	// достаточно первых 64 KiB ответа.
	const maxCapturedBodySize = 64 * 1024

	if recorder.body.Len() < maxCapturedBodySize {
		remainingSize :=
			maxCapturedBodySize - recorder.body.Len()

		if len(data) > remainingSize {
			recorder.body.Write(data[:remainingSize])
		} else {
			recorder.body.Write(data)
		}
	}

	return recorder.ResponseWriter.Write(data)
}

// Unwrap возвращает исходный ResponseWriter.
func (recorder *loggingResponseWriter) Unwrap() http.ResponseWriter {
	return recorder.ResponseWriter
}

// emitRequestResultLog создаёт отдельное событие CRUD или ошибки.
func emitRequestResultLog(
	r *http.Request,
	recorder *loggingResponseWriter,
	statusCode int,
) {
	fields := map[string]any{
		"method": r.Method,
		"path":   r.URL.Path,
		"ip":     clientIP(r),
		"status": statusCode,
	}

	responseMessage :=
		responseErrorMessage(recorder.body.Bytes())

	if responseMessage != "" {
		fields["error"] = responseMessage
	}

	switch {
	case statusCode == http.StatusBadRequest:
		fields["event"] = "validation_error"

		emitStructuredLog(
			"WARN",
			"Ошибка валидации входных данных",
			fields,
		)

		return

	case statusCode == http.StatusConflict:
		fields["event"] = "user_conflict"

		emitStructuredLog(
			"WARN",
			"Конфликт данных пользователя",
			fields,
		)

		return

	case statusCode == http.StatusNotFound:
		fields["event"] = "resource_not_found"

		if userID, ok := userIDFromPath(r.URL.Path); ok {
			fields["user_id"] = userID
		}

		emitStructuredLog(
			"WARN",
			"Запрошенный ресурс не найден",
			fields,
		)

		return

	case statusCode >= http.StatusInternalServerError:
		fields["event"] = "application_error"

		emitStructuredLog(
			"ERROR",
			"Ошибка приложения или базы данных",
			fields,
		)

		return
	}

	if statusCode < http.StatusOK ||
		statusCode >= http.StatusMultipleChoices {
		return
	}

	switch {
	case r.Method == http.MethodPost &&
		r.URL.Path == "/user":

		fields["event"] = "user_created"
		fields["operation"] = "create"

		if userID, ok :=
			userIDFromResponse(recorder.body.Bytes()); ok {
			fields["user_id"] = userID
		}

		emitStructuredLog(
			"INFO",
			"Пользователь создан",
			fields,
		)

	case r.Method == http.MethodPut &&
		strings.HasPrefix(r.URL.Path, "/user/"):

		fields["event"] = "user_updated"
		fields["operation"] = "update"

		if userID, ok := userIDFromPath(r.URL.Path); ok {
			fields["user_id"] = userID
		}

		emitStructuredLog(
			"INFO",
			"Пользователь обновлён",
			fields,
		)

	case r.Method == http.MethodDelete &&
		strings.HasPrefix(r.URL.Path, "/user/"):

		fields["event"] = "user_deleted"
		fields["operation"] = "delete"

		if userID, ok := userIDFromPath(r.URL.Path); ok {
			fields["user_id"] = userID
		}

		emitStructuredLog(
			"INFO",
			"Пользователь удалён",
			fields,
		)

	case r.Method == http.MethodGet &&
		r.URL.Path == "/user":

		fields["event"] = "users_listed"
		fields["operation"] = "read"

		emitStructuredLog(
			"INFO",
			"Список пользователей получен",
			fields,
		)

	case r.Method == http.MethodGet &&
		strings.HasPrefix(r.URL.Path, "/user/"):

		fields["event"] = "user_read"
		fields["operation"] = "read"

		if userID, ok := userIDFromPath(r.URL.Path); ok {
			fields["user_id"] = userID
		}

		emitStructuredLog(
			"INFO",
			"Пользователь получен",
			fields,
		)
	}
}

// emitStructuredLog выводит одну JSON-строку в stdout.
func emitStructuredLog(
	level string,
	message string,
	fields map[string]any,
) {
	record := map[string]any{
		"timestamp": time.Now().
			UTC().
			Format(time.RFC3339Nano),

		"level": strings.ToUpper(level),

		"service": getEnv(
			"SERVICE_NAME",
			defaultServiceName,
		),

		"message": message,
	}

	for key, value := range fields {
		record[key] = normalizeLogValue(value)
	}

	encodedRecord, err := json.Marshal(record)
	if err != nil {
		encodedRecord = []byte(fmt.Sprintf(
			`{"timestamp":%q,"level":"ERROR","service":%q,"message":"Не удалось сериализовать лог","error":%q}`,
			time.Now().UTC().Format(time.RFC3339Nano),
			getEnv(
				"SERVICE_NAME",
				defaultServiceName,
			),
			err.Error(),
		))
	}

	structuredLogMutex.Lock()
	defer structuredLogMutex.Unlock()

	_, _ = os.Stdout.Write(
		append(encodedRecord, '\n'),
	)
}

// normalizeLogValue преобразует ошибки в читаемые строки.
func normalizeLogValue(value any) any {
	switch typedValue := value.(type) {
	case error:
		return typedValue.Error()

	case fmt.Stringer:
		return typedValue.String()

	default:
		return value
	}
}

// clientIP возвращает IP клиента с учётом Ingress/reverse proxy.
func clientIP(r *http.Request) string {
	forwardedFor :=
		strings.TrimSpace(
			r.Header.Get("X-Forwarded-For"),
		)

	if forwardedFor != "" {
		firstAddress, _, _ :=
			strings.Cut(forwardedFor, ",")

		return strings.TrimSpace(firstAddress)
	}

	realIP :=
		strings.TrimSpace(
			r.Header.Get("X-Real-IP"),
		)

	if realIP != "" {
		return realIP
	}

	host, _, err :=
		net.SplitHostPort(
			strings.TrimSpace(r.RemoteAddr),
		)

	if err == nil {
		return host
	}

	return strings.TrimSpace(r.RemoteAddr)
}

// userIDFromPath получает ID из адреса /user/{userId}.
func userIDFromPath(path string) (int64, bool) {
	value := strings.TrimPrefix(path, "/user/")

	if value == path ||
		value == "" ||
		strings.Contains(value, "/") {
		return 0, false
	}

	userID, err :=
		strconv.ParseInt(value, 10, 64)

	if err != nil || userID <= 0 {
		return 0, false
	}

	return userID, true
}

// userIDFromResponse получает ID из JSON-ответа.
func userIDFromResponse(body []byte) (int64, bool) {
	var response struct {
		ID int64 `json:"id"`
	}

	if err := json.Unmarshal(
		body,
		&response,
	); err != nil || response.ID <= 0 {
		return 0, false
	}

	return response.ID, true
}

// responseErrorMessage получает message из ErrorResponse.
func responseErrorMessage(body []byte) string {
	var response ErrorResponse

	if err := json.Unmarshal(
		body,
		&response,
	); err != nil {
		return ""
	}

	return strings.TrimSpace(response.Message)
}
