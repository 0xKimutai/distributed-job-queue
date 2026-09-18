package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/distributed-job-queue/config"
	"github.com/distributed-job-queue/internal/backoff"
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

func (w *Worker) executeJob(ctx context.Context, job *models.Job) {
	start := time.Now()
	slog.Info("executing job", "job_id", job.JobID, "task", job.TaskName)

	// Start a heartbeat goroutine that renews the lease while this job runs.
	// It runs concurrently with the job execution and stops when the job finishes.
	//
	// We use a separate cancelable context so we can stop the heartbeat
	// the moment the job completes — not just when the whole worker shuts down.
	heartbeatCtx, stopHeartbeat := context.WithCancel(ctx)
	defer stopHeartbeat() // always stop heartbeat when executeJob returns

	go w.runHeartbeat(heartbeatCtx, job.JobID.String())

	err := w.dispatch(ctx, job)
	duration := time.Since(start)
	dbCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err != nil {
		if backoff.IsPermanent(err) {
			slog.Error("permanent failure, not retrying", "job_id", job.JobID, "error", err, "duration", duration.String())
			if failErr := w.queue.Fail(dbCtx, job.JobID.String(), err); failErr != nil {
				slog.Error("failed to mark job as permanently failed", "job_id", job.JobID, "error", failErr)
			}
		} else if job.RetryCount >= job.MaxRetries {
			slog.Error("max retries exceeded, sending to dead letter", "job_id", job.JobID, "retry_count", job.RetryCount, "error", err)
			if failErr := w.queue.Fail(dbCtx, job.JobID.String(), err); failErr != nil {
				slog.Error("failed to mark job as failed", "job_id", job.JobID, "error", failErr)
			}
		} else {
			delay := backoff.Calculate(job.RetryCount, backoff.DefaultBase, backoff.DefaultMaxDelay)
			slog.Warn("transient failure, retrying", "job_id", job.JobID, "retry_count", job.RetryCount, "delay", delay.String(), "error", err)
			if retryErr := w.queue.Retry(dbCtx, job.JobID.String(), err, delay); retryErr != nil {
				slog.Error("failed to schedule retry", "job_id", job.JobID, "error", retryErr)
			}
		}
		return
	}

	if completeErr := w.queue.Complete(dbCtx, job.JobID.String()); completeErr != nil {
		slog.Error("failed to mark job as complete", "job_id", job.JobID, "error", completeErr)
		return
	}

	slog.Info("job completed", "job_id", job.JobID, "task", job.TaskName, "duration", duration.String())
}

// runHeartbeat periodically renews the lease for a running job.
// It runs in its own goroutine and exits when ctx is cancelled
// (which happens when executeJob returns via defer stopHeartbeat()).
//
// The heartbeat interval must be well under the lease duration — we use
// the configured WorkerHeartbeatInterval (default: 10s with a 30s lease).
// That gives 3 heartbeat chances before expiry, making false-positive
// expiries from brief scheduler pauses extremely unlikely.
func (w *Worker) runHeartbeat(ctx context.Context, jobID string) {
	ticker := time.NewTicker(w.cfg.WorkerHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Job finished — stop heartbeating.
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := w.queue.RenewLease(renewCtx, jobID, w.id, w.cfg.WorkerLeaseDuration)
			cancel()
			if err != nil {
				// Log but don't crash — a missed heartbeat isn't fatal immediately.
				// The lease gives us several more intervals before expiry.
				slog.Warn("failed to renew lease", "job_id", jobID, "worker_id", w.id, "error", err)
			} else {
				slog.Debug("lease renewed", "job_id", jobID, "worker_id", w.id)
			}
		}
	}
}

// RunRecoverySweep periodically scans for jobs whose lease has expired and
// resets them to pending so another worker can claim them.
//
// This is the mechanism that recovers jobs orphaned by crashed workers.
// It runs as a separate goroutine in the worker process — not in the API server
// (which should be stateless) and not as a separate binary (which adds
// operational complexity and a new failure mode).
//
// Called from cmd/worker/main.go alongside the poll-loop goroutines.
func (w *Worker) RunRecoverySweep(ctx context.Context) {
	// Sweep interval is shorter than lease duration so we catch expired
	// leases promptly. Worst-case recovery = lease_duration + sweep_interval.
	sweepInterval := w.cfg.WorkerLeaseDuration / 2
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	slog.Info("recovery sweep started", "interval", sweepInterval)

	for {
		select {
		case <-ctx.Done():
			slog.Info("recovery sweep stopped")
			return
		case <-ticker.C:
			recovered, err := w.queue.RecoverStaleJobs(ctx)
			if err != nil {
				slog.Error("recovery sweep failed", "error", err)
				continue
			}
			if recovered > 0 {
				slog.Warn("recovered stale jobs", "count", recovered)
			}
		}
	}
}
// For now we only have one fake handler. In Phase 3+ we'll register real handlers.
func (w *Worker) dispatch(ctx context.Context, job *models.Job) error {
	switch job.TaskName {
	case "send_email":
		return handleSendEmail(ctx, job)
	case "flaky_task":
		return handleFlakyTask(ctx, job)
	case "slow_task":
		return handleSlowTask(ctx, job)
	default:
		return backoff.Permanent(fmt.Errorf("unknown task: %s", job.TaskName))
	}
}

// handleFlakyTask simulates a transient failure — succeeds only on the 3rd attempt.
// This lets us observe the retry + backoff path end-to-end.
func handleFlakyTask(ctx context.Context, job *models.Job) error {
	if job.RetryCount < 2 {
		return fmt.Errorf("simulated transient error (attempt %d)", job.RetryCount+1)
	}
	slog.Info("flaky task succeeded after retries", "job_id", job.JobID, "retry_count", job.RetryCount)
	return nil
}
// handleSlowTask simulates a long-running job (60s) so we can kill the worker
// mid-execution and observe lease expiry + recovery sweep in action.
func handleSlowTask(ctx context.Context, job *models.Job) error {
	slog.Info("slow task started, will run for 60s", "job_id", job.JobID)
	select {
	case <-time.After(60 * time.Second):
		slog.Info("slow task completed", "job_id", job.JobID)
		return nil
	case <-ctx.Done():
		return fmt.Errorf("slow task cancelled: %w", ctx.Err())
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
