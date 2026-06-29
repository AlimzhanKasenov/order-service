package main

import (
	"encoding/json"
	"log"
	"net/http"
)

type HealthResponse struct {
	Status string `json:"status"`
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	// Разрешаем только GET-запросы.
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Поддерживаем только нужные пути без редиректа:
	// /health
	// /health/
	if r.URL.Path != "/health" && r.URL.Path != "/health/" {
		http.NotFound(w, r)
		return
	}

	// Отдаём JSON-ответ по заданию.
	w.Header().Set("Content-Type", "application/json")

	response := HealthResponse{
		Status: "OK",
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
}

func main() {
	// Вешаем обработчик на корень, чтобы Go ServeMux не делал автоматический redirect
	// с /health на /health/.
	mux := http.NewServeMux()
	mux.HandleFunc("/", healthHandler)

	addr := ":8000"

	log.Printf("Order service started on port %s", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
