package models

import (
	"time"

	"github.com/google/uuid"
)

// Status represents the lifecycle state of a job.
// Using typed constants instead of raw strings means the compiler catches typos.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

// Job mirrors the jobs table. Every field maps to a column.
// pgx scans rows directly into this struct using pgx's built-in scanner.
//
// Pointer types (*string, *time.Time) represent nullable columns —
// a nil pointer means SQL NULL. Never use sql.NullString / sql.NullTime
// with pgx; it handles Go pointers natively.
type Job struct {
	JobID          uuid.UUID  `db:"job_id"`
	QueueName      string     `db:"queue_name"`
	TaskName       string     `db:"task_name"`
	Payload        []byte     `db:"payload"` // raw JSONB bytes
	Status         Status     `db:"status"`
	Priority       int        `db:"priority"`
	MaxRetries     int        `db:"max_retries"`
	RetryCount     int        `db:"retry_count"`
	LastError      *string    `db:"last_error"`
	WorkerID       *string    `db:"worker_id"`
	RunAt          time.Time  `db:"run_at"`
	CreatedAt      time.Time  `db:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at"`
	LeaseExpiresAt *time.Time `db:"lease_expires_at"`
}
