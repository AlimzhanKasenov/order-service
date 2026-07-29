package main

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// Общее количество HTTP-запросов.
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "order_service",
			Subsystem: "http",
			Name:      "requests_total",
			Help:      "Общее количество HTTP-запросов к order-service.",
		},
		[]string{"method", "route", "status"},
	)

	// Время выполнения HTTP-запросов.
	httpRequestDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "order_service",
			Subsystem: "http",
			Name:      "request_duration_seconds",
			Help:      "Время выполнения HTTP-запросов к order-service в секундах.",
			Buckets: []float64{
				0.005,
				0.01,
				0.025,
				0.05,
				0.1,
				0.25,
				0.5,
				1,
				2.5,
				5,
				10,
			},
		},
		[]string{"method", "route", "status"},
	)

	// Количество запросов, выполняющихся прямо сейчас.
	httpRequestsInFlight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "order_service",
			Subsystem: "http",
			Name:      "requests_in_flight",
			Help:      "Количество HTTP-запросов, выполняющихся в данный момент.",
		},
	)
)

func init() {
	prometheus.MustRegister(
		httpRequestsTotal,
		httpRequestDurationSeconds,
		httpRequestsInFlight,
	)
}

// registerMonitoringRoutes регистрирует endpoint метрик и тестовые методы.
//
// /debug/slow используется для проверки графика latency и алерта.
// /debug/error используется для проверки графика 500-х ошибок и алерта.
func registerMonitoringRoutes(mux *http.ServeMux, app *Application) {
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /debug/slow", app.debugSlowHandler)
	mux.HandleFunc("GET /debug/error", app.debugErrorHandler)
}

// debugSlowHandler создаёт контролируемую задержку для демонстрации latency.
//
// Пример:
// GET /debug/slow?delay_ms=1000
func (app *Application) debugSlowHandler(w http.ResponseWriter, r *http.Request) {
	const (
		defaultDelayMilliseconds = 750
		maxDelayMilliseconds     = 5000
	)

	delayMilliseconds := defaultDelayMilliseconds

	if value := strings.TrimSpace(r.URL.Query().Get("delay_ms")); value != "" {
		parsedDelay, err := strconv.Atoi(value)
		if err != nil || parsedDelay < 0 || parsedDelay > maxDelayMilliseconds {
			writeError(
				w,
				http.StatusBadRequest,
				"delay_ms must be an integer between 0 and 5000",
			)
			return
		}

		delayMilliseconds = parsedDelay
	}

	time.Sleep(time.Duration(delayMilliseconds) * time.Millisecond)

	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "OK",
		"delayMs": delayMilliseconds,
	})
}

// debugErrorHandler создаёт контролируемый HTTP 500 для проверки Error Rate.
func (app *Application) debugErrorHandler(w http.ResponseWriter, _ *http.Request) {
	writeError(
		w,
		http.StatusInternalServerError,
		"test internal server error",
	)
}

// statusRecorder запоминает HTTP-код ответа обработчика.
type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader сохраняет первый отправленный HTTP-код.
func (recorder *statusRecorder) WriteHeader(statusCode int) {
	if recorder.statusCode != 0 {
		return
	}

	recorder.statusCode = statusCode
	recorder.ResponseWriter.WriteHeader(statusCode)
}

// Write устанавливает 200 OK, если обработчик ещё не отправил HTTP-код.
func (recorder *statusRecorder) Write(data []byte) (int, error) {
	if recorder.statusCode == 0 {
		recorder.WriteHeader(http.StatusOK)
	}

	return recorder.ResponseWriter.Write(data)
}

// Unwrap позволяет стандартным HTTP-механизмам получить исходный ResponseWriter.
func (recorder *statusRecorder) Unwrap() http.ResponseWriter {
	return recorder.ResponseWriter
}

// prometheusMiddleware собирает количество запросов, статус и latency.
func prometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Сам endpoint /metrics не включаем в прикладную статистику,
		// иначе запросы Prometheus будут искажать RPS сервиса.
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		startedAt := time.Now()
		recorder := &statusRecorder{
			ResponseWriter: w,
		}

		httpRequestsInFlight.Inc()

		defer func() {
			httpRequestsInFlight.Dec()

			statusCode := recorder.statusCode
			if statusCode == 0 {
				statusCode = http.StatusOK
			}

			labels := prometheus.Labels{
				"method": r.Method,
				"route":  metricRoute(r),
				"status": strconv.Itoa(statusCode),
			}

			httpRequestsTotal.With(labels).Inc()
			httpRequestDurationSeconds.
				With(labels).
				Observe(time.Since(startedAt).Seconds())
		}()

		next.ServeHTTP(recorder, r)
	})
}

// metricRoute возвращает шаблон маршрута вместо реального URL.
//
// Благодаря этому запросы /user/1, /user/2 и /user/500
// попадут в одну серию /user/{userId}, а не создадут сотни метрик.
func metricRoute(r *http.Request) string {
	pattern := strings.TrimSpace(r.Pattern)

	if pattern != "" {
		if _, route, found := strings.Cut(pattern, " "); found {
			if route == "/health/{$}" {
				return "/health"
			}

			return route
		}

		return pattern
	}

	switch {
	case r.URL.Path == "/health", r.URL.Path == "/health/":
		return "/health"

	case r.URL.Path == "/user":
		return "/user"

	case strings.HasPrefix(r.URL.Path, "/user/"):
		return "/user/{userId}"

	case r.URL.Path == "/debug/slow":
		return "/debug/slow"

	case r.URL.Path == "/debug/error":
		return "/debug/error"

	default:
		return "unmatched"
	}
}
