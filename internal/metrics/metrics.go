package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// All metrics are registered with promauto, which auto-registers them with
// the default Prometheus registry on package init. No manual registration needed.
//
// Naming convention: <namespace>_<subsystem>_<name>_<unit>
// Units are always in base units: seconds (not ms), bytes (not KB).

var (
	// JobsEnqueued counts every successful job insertion.
	// Label "queue_name" lets you slice by queue.
	JobsEnqueued = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobqueue",
			Subsystem: "jobs",
			Name:      "enqueued_total",
			Help:      "Total number of jobs enqueued.",
		},
		[]string{"queue_name", "task_name"},
	)

	// JobsCompleted counts successfully completed jobs.
	JobsCompleted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobqueue",
			Subsystem: "jobs",
			Name:      "completed_total",
			Help:      "Total number of jobs completed successfully.",
		},
		[]string{"queue_name", "task_name"},
	)

	// JobsFailed counts permanently failed jobs (dead letter).
	JobsFailed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobqueue",
			Subsystem: "jobs",
			Name:      "failed_total",
			Help:      "Total number of jobs permanently failed.",
		},
		[]string{"queue_name", "task_name"},
	)

	// JobsRetried counts jobs scheduled for retry (transient failures).
	JobsRetried = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "jobqueue",
			Subsystem: "jobs",
			Name:      "retried_total",
			Help:      "Total number of job retry attempts scheduled.",
		},
		[]string{"queue_name", "task_name"},
	)

	// JobsRecovered counts jobs recovered from crashed workers by the sweep.
	JobsRecovered = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "jobqueue",
			Subsystem: "jobs",
			Name:      "recovered_total",
			Help:      "Total number of stale jobs recovered by the recovery sweep.",
		},
	)

	// JobExecutionDuration tracks how long job execution takes.
	// A histogram lets you compute p50, p95, p99 latencies.
	// Buckets are in seconds: 10ms, 50ms, 100ms, 500ms, 1s, 5s, 30s, 60s, 5min.
	JobExecutionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "jobqueue",
			Subsystem: "jobs",
			Name:      "execution_duration_seconds",
			Help:      "Job execution duration in seconds.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 30, 60, 300},
		},
		[]string{"queue_name", "task_name", "status"}, // status: completed|failed|retried
	)

	// ClaimDuration tracks how long the claim query takes.
	// Spikes here indicate database lock contention or slow queries.
	ClaimDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "jobqueue",
			Subsystem: "db",
			Name:      "claim_duration_seconds",
			Help:      "Duration of the job claim query in seconds.",
			Buckets:   prometheus.DefBuckets, // 5ms to 10s
		},
	)
)
