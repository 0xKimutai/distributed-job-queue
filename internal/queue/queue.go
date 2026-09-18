package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/distributed-job-queue/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Queue wraps the database pool and exposes job queue operations.
// Both the API (enqueue) and workers (claim, complete, fail) use this.
// Keeping SQL here rather than in handlers/workers means one place to audit.
type Queue struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

// Enqueue inserts a new job and returns its ID.
// Called by the API handler.
func (q *Queue) Enqueue(ctx context.Context, taskName, queueName string, payload []byte, priority int) (string, error) {
	const sql = `
		INSERT INTO jobs (task_name, queue_name, payload, priority)
		VALUES ($1, $2, $3, $4)
		RETURNING job_id
	`
	var jobID string
	err := q.pool.QueryRow(ctx, sql, taskName, queueName, payload, priority).Scan(&jobID)
	if err != nil {
		return "", fmt.Errorf("enqueue: %w", err)
	}
	return jobID, nil
}

// Claim atomically finds the highest-priority pending job and marks it running.
// Returns pgx.ErrNoRows if no jobs are available — this is NOT an error,
// it means the worker should sleep and try again.
//
// The leaseDuration controls how long the worker has before another worker
// may reclaim the job if no heartbeat is received.
func (q *Queue) Claim(ctx context.Context, workerID string, leaseDuration time.Duration) (*models.Job, error) {
	const sql = `
		UPDATE jobs
		SET
			status           = 'running',
			worker_id        = $1,
			lease_expires_at = now() + $2::interval
		WHERE job_id = (
			SELECT job_id
			FROM   jobs
			WHERE  status = 'pending'
			  AND  run_at <= now()
			ORDER  BY priority DESC, created_at ASC
			FOR UPDATE SKIP LOCKED
			LIMIT  1
		)
		RETURNING
			job_id, queue_name, task_name, payload, status, priority,
			max_retries, retry_count, last_error, worker_id,
			run_at, created_at, updated_at, lease_expires_at
	`

	rows, err := q.pool.Query(ctx, sql, workerID, leaseDuration.String())
	if err != nil {
		return nil, fmt.Errorf("claim query: %w", err)
	}
	defer rows.Close()

	job, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[models.Job])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, pgx.ErrNoRows // caller handles this as "nothing to do"
		}
		return nil, fmt.Errorf("claim scan: %w", err)
	}

	return &job, nil
}

// Complete marks a job as successfully finished.
func (q *Queue) Complete(ctx context.Context, jobID string) error {
	const sql = `
		UPDATE jobs
		SET status = 'completed', lease_expires_at = NULL, worker_id = NULL
		WHERE job_id = $1
	`
	_, err := q.pool.Exec(ctx, sql, jobID)
	if err != nil {
		return fmt.Errorf("complete job %s: %w", jobID, err)
	}
	return nil
}

// Retry resets a job back to pending with a future run_at (backoff delay)
// and increments the retry count. This is called when a job fails transiently
// and has not yet exceeded max_retries.
//
// The job becomes invisible to workers until run_at passes — this is how
// exponential backoff is enforced without any scheduler or timer process.
// The existing claim query already filters on run_at <= now().
func (q *Queue) Retry(ctx context.Context, jobID string, jobErr error, delay time.Duration) error {
	const sql = `
		UPDATE jobs
		SET
			status           = 'pending',
			worker_id        = NULL,
			lease_expires_at = NULL,
			retry_count      = retry_count + 1,
			last_error       = $2,
			run_at           = now() + $3::interval
		WHERE job_id = $1
	`
	errMsg := jobErr.Error()
	_, err := q.pool.Exec(ctx, sql, jobID, errMsg, delay.String())
	if err != nil {
		return fmt.Errorf("retry job %s: %w", jobID, err)
	}
	return nil
}
// In Phase 4 we'll add retry logic here — for now it goes straight to failed.
func (q *Queue) Fail(ctx context.Context, jobID string, jobErr error) error {
	const sql = `
		UPDATE jobs
		SET
			status           = 'failed',
			last_error       = $2,
			lease_expires_at = NULL,
			worker_id        = NULL
		WHERE job_id = $1
	`
	errMsg := jobErr.Error()
	_, err := q.pool.Exec(ctx, sql, jobID, errMsg)
	if err != nil {
		return fmt.Errorf("fail job %s: %w", jobID, err)
	}
	return nil
}
