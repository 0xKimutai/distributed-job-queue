# Distributed Job Queue

A production-style distributed job processing system built as a serious systems-engineering learning project. PostgreSQL-backed, written in Go, designed to be understood — not just used.

---

## What This Is

A job queue where producers enqueue work via an HTTP API and multiple concurrent workers compete to claim and execute that work. The interesting part is doing this **correctly** under concurrent workers, worker crashes, network failures, and high load.

This is not a toy. Every design decision here — from `FOR UPDATE SKIP LOCKED` to exponential backoff with jitter to the two-context graceful shutdown pattern — reflects how production systems at companies like Stripe, Uber, and GitHub actually work.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    HTTP API Server                       │
│              POST /jobs  GET /health  GET /metrics       │
└──────────────────────────┬──────────────────────────────┘
                           │  INSERT INTO jobs
                           ▼
┌─────────────────────────────────────────────────────────┐
│                      PostgreSQL                         │
│   jobs table — status, priority, lease, retry state     │
└──────────┬──────────────────────────────┬───────────────┘
           │  FOR UPDATE SKIP LOCKED      │  FOR UPDATE SKIP LOCKED
           ▼                              ▼
┌──────────────────────┐      ┌──────────────────────┐
│     Worker Pool      │      │     Worker Pool      │
│  goroutine-0  ──┐    │      │  goroutine-0  ──┐    │
│  goroutine-1  ──┼──► │      │  goroutine-1  ──┼──► │
│  goroutine-2  ──┘    │      │  goroutine-2  ──┘    │
│  sweeper goroutine   │      │  sweeper goroutine   │
└──────────────────────┘      └──────────────────────┘
```

---

## Features

- **Atomic job claiming** — `UPDATE ... FOR UPDATE SKIP LOCKED` prevents two workers from processing the same job
- **Priority queue with aging** — higher priority jobs run first; old jobs gain priority over time to prevent starvation
- **Exponential backoff with jitter** — failed jobs retry with randomized delays to prevent retry storms
- **Permanent vs transient errors** — handlers can signal non-retryable failures to skip the retry cycle entirely
- **Lease-based ownership** — workers hold time-bounded leases; crashed workers are automatically recovered
- **Heartbeat renewal** — workers renew leases periodically; missed heartbeats signal crashes
- **Recovery sweep** — background goroutine reclaims orphaned jobs from crashed workers
- **Graceful shutdown** — SIGTERM stops new work, lets in-flight jobs finish, writes final state using a separate cleanup context
- **Prometheus metrics** — counters and histograms for jobs enqueued, completed, failed, retried, and recovered; DB claim latency
- **Liveness and readiness probes** — `/health/live` and `/health/ready` for orchestrators and load balancers
- **pprof profiling** — `/debug/pprof/` for live goroutine and CPU profiling under load
- **Connection pooling** — `pgxpool` with configurable `MaxConns`, idle timeout, and connect timeout

---

## Project Structure

```
.
├── cmd/
│   ├── api/            # HTTP API server binary
│   └── worker/         # Worker binary
├── config/             # Configuration loading from environment variables
├── deployments/
│   └── docker/         # Docker Compose for local PostgreSQL
├── docs/
│   └── learning-journal.md  # Concepts learned, design decisions, questions
├── internal/
│   ├── api/            # HTTP handlers and router
│   ├── backoff/        # Exponential backoff with jitter, ErrPermanent sentinel
│   ├── db/             # Connection pool and migration runner
│   ├── metrics/        # Prometheus metric definitions
│   ├── models/         # Job struct and status constants
│   ├── queue/          # Core queue operations (enqueue, claim, complete, fail, retry)
│   └── worker/         # Poll loop, heartbeat, recovery sweep, job dispatch
├── migrations/         # SQL migration files (up + down)
├── scripts/
│   ├── loadtest.go     # Concurrent load test
│   └── slowdb_test.sh  # DB connection exhaustion experiment
└── .env                # Local development environment variables
```

---

## Getting Started

### Prerequisites

- Go 1.21+
- PostgreSQL 14+ (local install or Docker)

### Database Setup

If using a local PostgreSQL:

```bash
sudo -u postgres psql -c "CREATE USER jobqueue WITH PASSWORD 'jobqueue_secret';"
sudo -u postgres psql -c "CREATE DATABASE jobqueue OWNER jobqueue;"
```

If using Docker:

```bash
docker compose -f deployments/docker/docker-compose.yml up -d
```

### Configuration

Copy the example env file and adjust if needed:

```bash
cp .env.example .env
```

Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | PostgreSQL connection string (required) |
| `API_PORT` | `8080` | HTTP server port |
| `WORKER_CONCURRENCY` | `3` | Number of concurrent worker goroutines |
| `WORKER_LEASE_DURATION` | `30s` | How long a claimed job lease lasts |
| `WORKER_HEARTBEAT_INTERVAL` | `10s` | How often workers renew their lease |
| `WORKER_POLL_INTERVAL` | `2s` | How often workers poll for new jobs |

### Running

Migrations run automatically on API server startup.

```bash
# Terminal 1 — API server
go run ./cmd/api/

# Terminal 2 — Worker
go run ./cmd/worker/
```

### Enqueue a Job

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"task_name": "send_email", "payload": {"to": "user@example.com"}, "priority": 5}'
```

Response:
```json
{"job_id": "8a1a3bd1-dd97-4960-9b7d-3a683a37293a"}
```

---

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/jobs` | Enqueue a new job |
| `GET` | `/health/live` | Liveness probe — is the process alive? |
| `GET` | `/health/ready` | Readiness probe — is the DB reachable? |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/debug/pprof/` | pprof profiling endpoints |

### POST /jobs

Request body:

```json
{
  "task_name":  "send_email",   // required
  "payload":    {"key": "val"}, // optional, defaults to {}
  "queue_name": "default",      // optional, defaults to "default"
  "priority":   5               // optional, defaults to 0 (higher = sooner)
}
```

---

## Job Lifecycle

```
pending → running → completed
                 ↘
                   pending (retry, run_at = now + backoff)
                 ↘
                   failed  (max retries exceeded or permanent error)
```

A job stuck in `running` with an expired lease is reset to `pending` by the recovery sweep. This handles worker crashes without incrementing `retry_count` — a crash is infrastructure failure, not an application failure.

---

## Metrics

All metrics are prefixed with `jobqueue_`.

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `jobqueue_jobs_enqueued_total` | Counter | `queue_name`, `task_name` | Jobs inserted |
| `jobqueue_jobs_completed_total` | Counter | `queue_name`, `task_name` | Jobs completed successfully |
| `jobqueue_jobs_failed_total` | Counter | `queue_name`, `task_name` | Jobs permanently failed |
| `jobqueue_jobs_retried_total` | Counter | `queue_name`, `task_name` | Transient failures scheduled for retry |
| `jobqueue_jobs_recovered_total` | Counter | — | Jobs recovered from crashed workers |
| `jobqueue_jobs_execution_duration_seconds` | Histogram | `queue_name`, `task_name`, `status` | Job execution duration |
| `jobqueue_db_claim_duration_seconds` | Histogram | — | Claim query latency |

---

## Load Testing

```bash
# 50 concurrent goroutines, 500 total requests
go run scripts/loadtest.go -concurrency=50 -total=500

# Custom URL
go run scripts/loadtest.go -concurrency=100 -total=2000 -url=http://localhost:8080
```

---

## Design Decisions

**Why PostgreSQL instead of Redis or RabbitMQ?**
Transactional enqueue — a job insert can happen in the same transaction as the business operation that created it. Either both commit or neither does. You also get full SQL visibility into the queue state. The tradeoff is throughput: Postgres queues start struggling above a few thousand jobs/sec at high concurrency.

**Why `FOR UPDATE SKIP LOCKED`?**
Plain `FOR UPDATE` causes waiting workers to block on a locked row, then wake up simultaneously when it's released (thundering herd). `SKIP LOCKED` tells a worker to skip locked rows and find the next available one — returning in microseconds rather than blocking for milliseconds.

**Why leases instead of locks?**
A database lock is released when the transaction commits or the connection closes. A lease is a time-bounded claim stored as a column. If a worker crashes, its connection closes but the lease column still holds the old expiry. The recovery sweep finds leases past their expiry and resets them. Locks can't do this.

**Why jitter on retries?**
Without jitter, all workers that fail on the same external service at T+0 retry at exactly T+2s. They all fail again. They all retry at T+4s. You've built a periodic DDoS against your own dependency. Jitter desynchronizes retries so a recovering service sees a trickle rather than a spike.

**Why two contexts in `executeJob`?**
`ctx` is cancelled on graceful shutdown — it signals "stop doing work." But after the job handler returns, we still need to write the job's final state to the database. Using a cancelled context for DB writes causes them to fail immediately, leaving the job orphaned in `running` status. `dbCtx` is a fresh `context.Background()` with a short timeout that lets cleanup writes succeed regardless of shutdown state.

---