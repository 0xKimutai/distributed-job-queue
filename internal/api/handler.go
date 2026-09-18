package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/distributed-job-queue/config"
	"github.com/distributed-job-queue/internal/queue"
	"github.com/jackc/pgx/v5/pgxpool"
)

type handler struct {
	pool  *pgxpool.Pool
	queue *queue.Queue
	cfg   *config.Config
}

// handleLive is a liveness probe — just confirms the process is running.
// No DB check. Kubernetes uses this to decide whether to restart the pod.
func (h *handler) handleLive(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady is a readiness probe — confirms the process can serve traffic.
// Checks DB connectivity. Load balancers use this to route traffic.
// Returns 503 if the DB is unreachable so the LB stops sending requests here.
func (h *handler) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if err := h.pool.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status":   "unavailable",
			"database": "unreachable",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status":   "ok",
		"database": "ok",
	})
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
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

	var req EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if req.TaskName == "" {
		writeError(w, http.StatusBadRequest, "task_name is required")
		return
	}

	if req.QueueName == "" {
		req.QueueName = "default"
	}
	if req.Payload == nil {
		req.Payload = json.RawMessage("{}")
	}

	// Use h.queue.Enqueue so the JobsEnqueued metric is recorded.
	jobID, err := h.queue.Enqueue(r.Context(), req.TaskName, req.QueueName, req.Payload, req.Priority)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

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
