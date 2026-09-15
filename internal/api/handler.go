package api

import (
	"encoding/json"
	"net/http"

	"github.com/distributed-job-queue/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

// handler holds shared dependencies for all HTTP handlers.
// This is the standard Go pattern: dependencies injected at construction,
// methods hang off the struct. No global state.
type handler struct {
	pool *pgxpool.Pool
	cfg  *config.Config
}

// handleHealth is a simple liveness probe.
// Load balancers and container orchestrators call this to check if the
// process is alive. We'll extend it to also check DB connectivity in Phase 8.
func (h *handler) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// EnqueueRequest is the JSON body for POST /jobs.
type EnqueueRequest struct {
	TaskName  string          `json:"task_name"`
	Payload   json.RawMessage `json:"payload"`
	QueueName string          `json:"queue_name"`
	Priority  int             `json:"priority"`
}

// EnqueueResponse is returned on successful job creation.
type EnqueueResponse struct {
	JobID string `json:"job_id"`
}

// handleEnqueueJob is your task to implement.
//
// It should:
//  1. Decode the JSON request body into EnqueueRequest
//  2. Validate that task_name is not empty
//  3. Apply defaults: if queue_name is empty, use "default"; if payload is null, use {}
//  4. INSERT a row into the jobs table, returning the job_id
//  5. Respond with 201 Created and {"job_id": "<uuid>"}
//  6. On error, respond with an appropriate HTTP status and JSON error body
//
// Use h.pool.QueryRow() for the INSERT ... RETURNING query.
// Use r.Context() as the context — it carries the request deadline.
func (h *handler) handleEnqueueJob(w http.ResponseWriter, r *http.Request) {
	// Limit request body to 1MB to prevent memory exhaustion from large payloads.
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	// Decode the request body.
	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		// Malformed JSON is a client mistake — 400, not 500.
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	// Validate required fields.
	if req.TaskName == "" {
		writeError(w, http.StatusBadRequest, "task_name is required")
		return
	}

	// Apply defaults.
	if req.QueueName == "" {
		req.QueueName = "default"
	}
	if req.Payload == nil {
		req.Payload = json.RawMessage("{}")
	}

	// INSERT and return the generated job_id.
	const query = `
		INSERT INTO jobs (task_name, queue_name, payload, priority)
		VALUES ($1, $2, $3, $4)
		RETURNING job_id
	`

	var jobID string
	err := h.pool.QueryRow(
		r.Context(),
		query,
		req.TaskName,
		req.QueueName,
		req.Payload,
		req.Priority,
	).Scan(&jobID)
	if err != nil {
		// Never leak raw DB errors to clients.
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	// jobID is already a string — no conversion needed.
	writeJSON(w, http.StatusCreated, EnqueueResponse{JobID: jobID})
}

// writeJSON is a helper to write a JSON response with a given status code.
// You'll use this in handleEnqueueJob and future handlers.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
