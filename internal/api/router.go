package api

import (
	"net/http"

	"github.com/distributed-job-queue/config"
	"github.com/distributed-job-queue/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRouter wires up all HTTP routes and returns the root handler.
func NewRouter(pool *pgxpool.Pool, cfg *config.Config) http.Handler {
	mux := http.NewServeMux()

	h := &handler{pool: pool, queue: queue.New(pool), cfg: cfg}

	// Liveness — is the process alive? No DB check, just returns 200.
	mux.HandleFunc("GET /health/live", h.handleLive)

	// Readiness — is the process ready to serve traffic? Checks DB.
	mux.HandleFunc("GET /health/ready", h.handleReady)

	// Legacy health endpoint — keep for backwards compatibility.
	mux.HandleFunc("GET /health", h.handleReady)

	// Prometheus metrics scrape endpoint.
	// promhttp.Handler() serves the default registry which includes all
	// metrics registered via promauto plus Go runtime metrics (GC, goroutines, memory).
	mux.Handle("GET /metrics", promhttp.Handler())

	mux.HandleFunc("POST /jobs", h.handleEnqueueJob)

	return mux
}
