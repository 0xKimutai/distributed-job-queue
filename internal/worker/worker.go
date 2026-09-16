package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/distributed-job-queue/config"
	"github.com/distributed-job-queue/internal/models"
	"github.com/distributed-job-queue/internal/queue"
	"github.com/jackc/pgx/v5"
)

// Worker polls the job queue and executes jobs one at a time.
// In Phase 5 we'll add a pool of concurrent workers. For now: one goroutine,
// one job at a time, so we can understand the basic lifecycle clearly.
type Worker struct {
	id    string
	queue *queue.Queue
	cfg   *config.Config
}

func New(id string, q *queue.Queue, cfg *config.Config) *Worker {
	return &Worker{id: id, queue: q, cfg: cfg}
}

// Run starts the poll loop. It blocks until ctx is cancelled.
// This is the entry point called from cmd/worker/main.go.
func (w *Worker) Run(ctx context.Context) {
	slog.Info("worker started", "worker_id", w.id)
	ticker := time.NewTicker(w.cfg.WorkerPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("worker shutting down", "worker_id", w.id)
			return
		case <-ticker.C:
			w.pollOnce(ctx)
		}
	}
}

// pollOnce attempts to claim and execute one job.
// It is called on every tick of the poll interval.
//
// YOUR TASK: implement this function.
//
// It should:
//  1. Call w.queue.Claim() to try to claim a job
//  2. If err == pgx.ErrNoRows: log at Debug level "no jobs available" and return
//  3. If err is any other error: log at Error level and return
//  4. If a job was claimed: log at Info level "job claimed", then call w.executeJob()
func (w *Worker) pollOnce(ctx context.Context) {
	// try to claim a job
	job, err := w.queue.Claim (
		ctx,
		w.id,
		w.cfg.WorkerLeaseDuration,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			slog.Debug("no jobs available", "worker_id", w.id)
			return
		}
		slog.Error("failed to claim job", "worker_id", w.id, "error", err)
		return
	}

	slog.Info("job claimed", "worker_id", w.id, "job_id", job.JobID, "task", job.TaskName)
	w.executeJob(ctx, job)
}

// executeJob runs the job and marks it complete or failed.
//
// YOUR TASK: implement this function.
//
// It should:
//  1. Log "executing job" with job_id and task_name
//  2. Call w.dispatch(ctx, job) to run the actual handler
//  3. If dispatch returns an error: call w.queue.Fail() and log the failure
//  4. If dispatch succeeds: call w.queue.Complete() and log success
//  5. Log how long execution took (use time.Since)
func (w *Worker) executeJob(ctx context.Context, job *models.Job) {
	start := time.Now()
	slog.Info("executing job", "job_id", job.JobID, "task", job.TaskName)

	err := w.dispatch(ctx, job)
	duration := time.Since(start)

	if err != nil {
		slog.Error("job failed", "job_id", job.JobID, "task", job.TaskName, "error", err, "duration", duration)
		if failErr := w.queue.Fail(ctx, job.JobID.String(), err); failErr != nil {
			slog.Error("failed to mark job as failed", "job_id", job.JobID, "error", failErr)
		}
		return
	}

	if completeErr := w.queue.Complete(ctx, job.JobID.String()); completeErr != nil {
		slog.Error("failed to mark job as complete", "job_id", job.JobID, "error", completeErr)
		return
	}

	slog.Info("job completed", "job_id", job.JobID, "task", job.TaskName, "duration", duration)
}

// dispatch routes a job to its handler based on task_name.
// For now we only have one fake handler. In Phase 3+ we'll register real handlers.
func (w *Worker) dispatch(ctx context.Context, job *models.Job) error {
	switch job.TaskName {
	case "send_email":
		return handleSendEmail(ctx, job)
	default:
		// Unknown task — this is a permanent failure, not a transient one.
		// We'll distinguish permanent vs transient failures in Phase 4.
		return fmt.Errorf("unknown task: %s", job.TaskName)
	}
}

// handleSendEmail is a fake job handler that simulates work.
// Real handlers will do actual work: call APIs, write to DBs, send emails, etc.
func handleSendEmail(ctx context.Context, job *models.Job) error {
	slog.Info("sending email", "job_id", job.JobID, "payload", string(job.Payload))
	// Simulate network call / work taking some time.
	time.Sleep(500 * time.Millisecond)
	slog.Info("email sent", "job_id", job.JobID)
	return nil
}
