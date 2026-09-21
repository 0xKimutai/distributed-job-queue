package queue

import (
	"context"
	"time"

	"github.com/distributed-job-queue/internal/models"
)

// Queue is the interface both the Postgres and Redis backends implement.
// Workers, the API handler, and the gRPC server all depend on this interface —
// not on a concrete implementation. This lets you swap backends or run both
// simultaneously without changing anything outside the queue package.
//
// This is the strategy pattern: the algorithm (how to claim, complete, fail)
// varies by implementation, but the contract is fixed.
type Queue interface {
	// Enqueue inserts a new job and returns its ID.
	Enqueue(ctx context.Context, taskName, queueName string, payload []byte, priority int) (string, error)

	// Claim atomically finds and claims the next pending job.
	// Returns pgx.ErrNoRows (Postgres) or a sentinel error (Redis) if empty.
	// Callers must check for this "nothing to do" case and not treat it as fatal.
	Claim(ctx context.Context, workerID string, leaseDuration time.Duration) (*models.Job, error)

	// Complete marks a job as successfully finished.
	Complete(ctx context.Context, jobID string) error

	// Fail marks a job as permanently failed and records the error.
	Fail(ctx context.Context, jobID string, jobErr error) error

	// Retry resets a job to pending with a future run_at for backoff.
	Retry(ctx context.Context, jobID string, jobErr error, delay time.Duration) error

	// RenewLease extends the lease expiry for a running job.
	RenewLease(ctx context.Context, jobID string, workerID string, leaseDuration time.Duration) error

	// RecoverStaleJobs resets running jobs with expired leases back to pending.
	// Returns the number of jobs recovered.
	RecoverStaleJobs(ctx context.Context) (int64, error)
}
