package api

import (
	"net/http"

	"github.com/distributed-job-queue/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewRouter wires up all HTTP routes and returns the root handler.
// We use the standard library's ServeMux — no external router framework.
// For this project, stdlib is sufficient and keeps dependencies minimal.
func NewRouter(pool *pgxpool.Pool, cfg *config.Config) http.Handler {
	mux := http.NewServeMux()

	h := &handler{pool: pool, cfg: cfg}

	mux.HandleFunc("GET /health", h.handleHealth)
	mux.HandleFunc("POST /jobs", h.handleEnqueueJob)

	return mux
}
