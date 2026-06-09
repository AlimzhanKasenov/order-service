package main

import (
	"encoding/json"
	"log"
	"net/http"
)

const serverAddress = ":8000"

type HealthResponse struct {
	Status string `json:"status"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health/", healthHandler)

	log.Printf("order-service запущен на порту %s", serverAddress)

	if err := http.ListenAndServe(serverAddress, mux); err != nil {
		log.Fatalf("Ошибка запуска HTTP-сервера: %v", err)
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/health/" {
		writeJSON(w, http.StatusNotFound, ErrorResponse{
			Error: "Not Found",
		})

		return
	}

	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, ErrorResponse{
			Error: "Method Not Allowed",
		})

		return
	}

	writeJSON(w, http.StatusOK, HealthResponse{
		Status: "OK",
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Ошибка формирования JSON-ответа: %v", err)
	}
}
