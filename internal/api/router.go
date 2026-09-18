package api

import (
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof handlers on http.DefaultServeMux

	"github.com/distributed-job-queue/config"
	"github.com/distributed-job-queue/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRouter wires up all HTTP routes and returns the root handler.
func NewRouter(pool *pgxpool.Pool, cfg *config.Config) http.Handler {
	mux := http.NewServeMux()

	h := &handler{pool: pool, queue: queue.New(pool), cfg: cfg}

	mux.HandleFunc("GET /health/live", h.handleLive)
	mux.HandleFunc("GET /health/ready", h.handleReady)
	mux.HandleFunc("GET /health", h.handleReady)
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("POST /jobs", h.handleEnqueueJob)

	// pprof profiling endpoints — useful for diagnosing CPU/memory under load.
	// In production, protect these behind authentication or a separate internal port.
	// Mounted from DefaultServeMux where net/http/pprof registers itself.
	mux.Handle("/debug/pprof/", http.DefaultServeMux)

	return mux
}
