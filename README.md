# Distributed Job Queue

A production-style distributed job processing system built as a serious systems-engineering learning project. PostgreSQL-backed, written in Go, with C++ workers via gRPC — designed to be understood, not just used.

---

## What This Is

A job queue where producers enqueue work via an HTTP API and multiple concurrent workers compete to claim and execute that work. The interesting part is doing this **correctly** under concurrent workers, worker crashes, network failures, and high load.

This is not a toy. Every design decision here — from `FOR UPDATE SKIP LOCKED` to exponential backoff with jitter to the two-context graceful shutdown pattern — reflects how production systems at companies like Stripe, Uber, and GitHub actually work.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                      HTTP API Server :8080                       │
│           POST /jobs   GET /health   GET /metrics                │
│                                                                  │
│                      gRPC Server :50051                          │
│           ClaimJob   CompleteJob   FailJob                       │
└────────────────────────────┬─────────────────────────────────────┘
                             │  INSERT INTO jobs
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                          PostgreSQL                              │
│      jobs table — status, priority, lease, retry state           │
└───────────┬──────────────────────────────────┬───────────────────┘
            │  FOR UPDATE SKIP LOCKED           │  gRPC ClaimJob RPC
            ▼                                  ▼
┌────────────────────────┐          ┌──────────────────────────┐
│    Go Worker Process   │          │    C++ Worker Process    │
│  goroutine-0           │          │  poll loop               │
│  goroutine-1           │          │  dispatch by task_name   │
│  goroutine-2           │          │  CompleteJob / FailJob   │
│  recovery sweeper      │          │                          │
└────────────────────────┘          └──────────────────────────┘
```

---

## Features

- **Atomic job claiming** — `UPDATE ... FOR UPDATE SKIP LOCKED` prevents two workers from processing the same job
- **Priority queue with aging** — higher priority jobs run first; old jobs gain priority over time to prevent starvation
- **Exponential backoff with jitter** — failed jobs retry with randomized delays to prevent retry storms
- **Permanent vs transient errors** — handlers signal non-retryable failures to skip the retry cycle entirely
- **Lease-based ownership** — workers hold time-bounded leases; crashed workers are automatically recovered
- **Heartbeat renewal** — workers renew leases periodically; missed heartbeats signal crashes
- **Recovery sweep** — background goroutine reclaims orphaned jobs from crashed workers
- **Graceful shutdown** — SIGTERM stops new work, lets in-flight jobs finish, writes final state via a separate cleanup context
- **gRPC interface** — C++ (and any other language) workers participate via a typed RPC contract, no direct DB access
- **Prometheus metrics** — counters and histograms for enqueue, complete, fail, retry, recover, and claim latency
- **Liveness and readiness probes** — `/health/live` and `/health/ready` for orchestrators and load balancers
- **pprof profiling** — `/debug/pprof/` for live goroutine and CPU profiling under load
- **Connection pooling** — `pgxpool` with configurable `MaxConns`, idle timeout, and connect timeout
- **systemd unit files** — proper Linux service management with restart policies and security hardening
- **Docker multi-stage builds** — ~18MB final images, non-root user, exec-form entrypoint

---

## Project Structure

```
.
├── cmd/
│   ├── api/                    # HTTP + gRPC API server binary
│   └── worker/                 # Go worker binary
├── config/                     # Configuration from environment variables
├── cpp-worker/
│   ├── proto/                  # Generated C++ protobuf/gRPC stubs
│   ├── worker.cpp              # C++ worker implementation
│   └── Makefile                # Build the C++ binary
├── deployments/
│   ├── docker/                 # Dockerfiles + Docker Compose
│   └── systemd/                # systemd unit files + install script
├── docs/
│   └── learning-journal.md     # Concepts learned, design decisions, questions
├── internal/
│   ├── api/                    # HTTP handlers and router
│   ├── backoff/                # Exponential backoff with jitter, ErrPermanent
│   ├── db/                     # Connection pool and migration runner
│   ├── grpc/
│   │   ├── pb/                 # Generated Go protobuf/gRPC stubs
│   │   └── server/             # gRPC server implementation
│   ├── metrics/                # Prometheus metric definitions
│   ├── models/                 # Job struct and status constants
│   ├── queue/                  # Core queue operations
│   └── worker/                 # Poll loop, heartbeat, recovery sweep
├── migrations/                 # SQL migration files (up + down)
├── proto/
│   └── jobqueue.proto          # Single source of truth for gRPC contract
├── scripts/
│   ├── gen_proto.sh            # Regenerate Go + C++ stubs from proto
│   ├── loadtest.go             # Concurrent load test
│   └── slowdb_test.sh          # DB connection exhaustion experiment
└── .env                        # Local development environment variables
```

---

## Getting Started

### Prerequisites

- Go 1.25+
- PostgreSQL 14+
- g++ with gRPC (for C++ worker): `sudo apt install libgrpc++-dev protobuf-compiler-grpc`

### Database Setup

Local PostgreSQL:

```bash
sudo -u postgres psql -c "CREATE USER jobqueue WITH PASSWORD 'jobqueue_secret';"
sudo -u postgres psql -c "CREATE DATABASE jobqueue OWNER jobqueue;"
```

Docker:

```bash
docker compose -f deployments/docker/docker-compose.yml up -d
```

### Configuration

```bash
cp .env.example .env
# Edit .env if your DB credentials differ
```

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | — | PostgreSQL connection string (required) |
| `API_PORT` | `8080` | HTTP server port |
| `GRPC_PORT` | `50051` | gRPC server port |
| `WORKER_CONCURRENCY` | `3` | Concurrent worker goroutines |
| `WORKER_LEASE_DURATION` | `30s` | Job lease duration |
| `WORKER_HEARTBEAT_INTERVAL` | `10s` | Lease renewal interval |
| `WORKER_POLL_INTERVAL` | `2s` | How often workers poll for jobs |

### Running

Migrations run automatically on API server startup.

```bash
# Terminal 1 — API + gRPC server
go run ./cmd/api/

# Terminal 2 — Go worker
go run ./cmd/worker/

# Terminal 3 (optional) — C++ worker
cd cpp-worker && make && ./jobqueue-cpp-worker localhost:50051 cpp-worker-1
```

### Enqueue a Job

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{"task_name": "send_email", "payload": {"to": "user@example.com"}, "priority": 5}'
```

---

## API Reference

### HTTP

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/jobs` | Enqueue a new job |
| `GET` | `/health/live` | Liveness probe |
| `GET` | `/health/ready` | Readiness probe — checks DB |
| `GET` | `/metrics` | Prometheus metrics |
| `GET` | `/debug/pprof/` | Live profiling |

### POST /jobs body

```json
{
  "task_name":  "send_email",
  "payload":    {"to": "user@example.com"},
  "queue_name": "default",
  "priority":   5
}
```

### gRPC (`:50051`)

Defined in `proto/jobqueue.proto`:

| RPC | Request | Response | Description |
|-----|---------|----------|-------------|
| `ClaimJob` | `ClaimJobRequest` | `ClaimJobResponse` | Atomically claims next job. Returns `NOT_FOUND` if empty |
| `CompleteJob` | `CompleteJobRequest` | `JobAck` | Marks job completed |
| `FailJob` | `FailJobRequest` | `JobAck` | Marks job failed or schedules retry |

### Regenerating gRPC Stubs

After editing `proto/jobqueue.proto` (adding fields, RPCs, or messages), regenerate the Go and C++ stubs:

```bash
bash scripts/gen_proto.sh
```

This runs `protoc` and overwrites the generated files in `internal/grpc/pb/` and `cpp-worker/proto/`. Never edit those files by hand — they'll be overwritten on the next generation.

**Prerequisites:**
```bash
# protoc compiler
sudo apt install protobuf-compiler protobuf-compiler-grpc

# Go plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

**Important when adding fields:** field numbers are permanent. Never reuse or renumber an existing field — old clients will misinterpret the data. Deprecate old fields and assign new numbers to new fields.

---

## Job Lifecycle

```
pending → running → completed
                 ↘
                   pending  (transient failure — run_at = now + backoff)
                 ↘
                   failed   (permanent error or max retries exceeded)
```

A job stuck in `running` with an expired lease is reset to `pending` by the recovery sweep. `retry_count` is not incremented — a worker crash is infrastructure failure, not an application failure.

---

## Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `jobqueue_jobs_enqueued_total` | Counter | `queue_name`, `task_name` | Jobs inserted |
| `jobqueue_jobs_completed_total` | Counter | `queue_name`, `task_name` | Completed successfully |
| `jobqueue_jobs_failed_total` | Counter | `queue_name`, `task_name` | Permanently failed |
| `jobqueue_jobs_retried_total` | Counter | `queue_name`, `task_name` | Scheduled for retry |
| `jobqueue_jobs_recovered_total` | Counter | — | Recovered from crashed workers |
| `jobqueue_jobs_execution_duration_seconds` | Histogram | `queue_name`, `task_name`, `status` | Execution duration |
| `jobqueue_db_claim_duration_seconds` | Histogram | — | Claim query latency |

---

## Load Testing

```bash
go run scripts/loadtest.go -concurrency=50 -total=500
go run scripts/loadtest.go -concurrency=100 -total=2000 -url=http://localhost:8080
```

---

## Observability

The full observability stack is Prometheus (metrics scraping + storage) + Grafana (dashboards). All config is pre-provisioned — no manual setup needed.

### How it connects

```
API server (:8080) → exposes /metrics in Prometheus text format
       ↑
Prometheus (:9090) → scrapes /metrics every 5s → stores as time series
       ↑
Grafana (:3000) → queries Prometheus → renders the dashboard
```

### Start the stack

```bash
docker compose -f deployments/observability/docker-compose.yml up -d
```

### Access the UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |

### View the dashboard

1. Open Grafana at http://localhost:3000
2. Navigate to **Dashboards → Job Queue → Distributed Job Queue**
3. The dashboard loads automatically — no import needed

The dashboard covers: enqueue rate, completion rate, failure rate, execution latency p50/p95/p99, claim query latency, retry rate, crash recovery events, goroutine count, and lifetime totals.

### Generate live metrics

```bash
# Start the services
go run ./cmd/api/
go run ./cmd/worker/

# Flood jobs and watch the graphs move
go run scripts/loadtest.go -concurrency=100 -total=2000
```

### Useful Prometheus queries

```promql
# Jobs per second (last 5 minutes)
rate(jobqueue_jobs_completed_total[5m])

# p99 execution latency
histogram_quantile(0.99, rate(jobqueue_jobs_execution_duration_seconds_bucket[2m]))

# Claim query p95 latency (DB health indicator)
histogram_quantile(0.95, rate(jobqueue_db_claim_duration_seconds_bucket[2m]))

# Total jobs ever enqueued
sum(jobqueue_jobs_enqueued_total)
```

### Stop the stack

```bash
# Stop containers (preserves metrics data)
docker compose -f deployments/observability/docker-compose.yml down

# Stop and delete all stored metrics
docker compose -f deployments/observability/docker-compose.yml down -v
```

---

## Production Deployment

### systemd

```bash
# Build binaries
go build -ldflags="-s -w" -o bin/jobqueue-api ./cmd/api/
go build -ldflags="-s -w" -o bin/jobqueue-worker ./cmd/worker/

# Install (as root)
sudo bash deployments/systemd/install.sh

# Manage
sudo systemctl start jobqueue-api jobqueue-worker
sudo journalctl -u jobqueue-api -f
```

### Docker

```bash
# Full stack
docker compose -f deployments/docker/docker-compose.yml up -d

# Scale workers horizontally
docker compose -f deployments/docker/docker-compose.yml up --scale worker=3
```

---

## Design Decisions

**Why PostgreSQL instead of Redis or RabbitMQ?**
Transactional enqueue — a job insert can happen in the same transaction as the business operation that created it. Either both commit or neither does. You also get full SQL visibility and joins. The tradeoff: Postgres queues start struggling above a few thousand jobs/sec at high concurrency.

**Why `FOR UPDATE SKIP LOCKED`?**
Plain `FOR UPDATE` causes waiting workers to block on a locked row, then wake simultaneously (thundering herd). `SKIP LOCKED` tells a worker to skip locked rows and find the next available one — returning in microseconds rather than blocking.

**Why leases instead of locks?**
A database lock releases when the connection closes. A lease is a time-bounded value in a column. If a worker crashes, its connection closes but the lease column holds the old expiry. The recovery sweep finds expired leases and resets them. Locks cannot do this.

**Why jitter on retries?**
Without jitter, all workers failing on the same external service at T+0 retry simultaneously at T+2s, fail again, and retry at T+4s — a periodic DDoS against your own dependency. Jitter spreads retries randomly across the window.

**Why two contexts in `executeJob`?**
`ctx` cancels on shutdown — "stop doing work." But DB writes after the job handler returns must still succeed. A cancelled context causes them to fail immediately, orphaning the job. `dbCtx` is a fresh context with a short timeout that survives the shutdown signal.

**Why gRPC for C++ workers?**
The C++ worker has zero database code. All queue semantics — atomic claiming, lease management, retry logic, backoff calculation — stay in Go. C++ calls `ClaimJob()` and gets a job. If queue internals change, only Go changes. Protobuf field-number stability means old and new workers interoperate safely during rolling deployments.

---

---

## Documentation

- [`docs/test-snapshots.md`](docs/test-snapshots.md) — real output from actual runs: worker logs, crash recovery timelines, load test numbers, backend benchmark results

---

## Roadmap

- [x] Phase 0 — Problem definition, schema, architecture
- [x] Phase 1 — Repository, migrations, API server
- [x] Phase 2 — First worker: poll loop, claim, execute
- [x] Phase 3 — Concurrent workers, `FOR UPDATE SKIP LOCKED`
- [x] Phase 4 — Retries, exponential backoff with jitter, dead letter
- [x] Phase 5 — Leases, heartbeats, stale job recovery
- [x] Phase 6 — Graceful shutdown, two-context pattern
- [x] Phase 7 — Priority aging, starvation prevention
- [x] Phase 8 — Observability: Prometheus metrics, health probes, pprof
- [x] Phase 9 — Connection pooling, load testing, resource exhaustion
- [x] Phase 10 — systemd unit files, Docker multi-stage builds
- [x] Phase 11 — C++ workers via gRPC and Protocol Buffers
- [x] Phase 12 — Redis backend, strategy pattern, benchmark comparison
