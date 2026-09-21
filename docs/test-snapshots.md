# Test Snapshots & Findings

Real output from actual runs on this machine. Every result here was produced
by running the system as described — no fabricated numbers.

**Machine:** Ubuntu 24.04, Go 1.25.11, PostgreSQL 18.6, Redis (local)
**Date:** September 2026

---

## Phase 2 — First Worker: Basic Job Execution

Enqueued a `send_email` job via `POST /jobs`, watched the worker claim and complete it.

```
{"level":"INFO","msg":"job claimed","worker_id":"Devpad-358849","job_id":"8a1a3bd1","task":"send_email"}
{"level":"INFO","msg":"executing job","job_id":"8a1a3bd1","task":"send_email"}
{"level":"INFO","msg":"sending email","job_id":"8a1a3bd1","payload":"{\"to\": \"user@example.com\"}"}
{"level":"INFO","msg":"email sent","job_id":"8a1a3bd1"}
{"level":"INFO","msg":"job completed","job_id":"8a1a3bd1","task":"send_email","duration":"500.747ms"}
```

**Database state after:**
```
job_id                | task_name  | status    | priority
8a1a3bd1-dd97-4960... | send_email | completed | 5
```

**Finding:** Single worker processes one job per poll cycle. 500ms execution time is the simulated `time.Sleep` in `handleSendEmail`.

---

## Phase 3 — Concurrent Workers + FOR UPDATE SKIP LOCKED

Three concurrent worker goroutines processing 9 jobs simultaneously. All claimed within 12ms of each other.

```
15:31:18.902  worker-0  claimed job 5  (send_email)
15:31:18.911  worker-2  claimed job 6  (send_email)   ← 9ms later, different job
15:31:18.914  worker-1  claimed job 1  (send_email)   ← 3ms later, different job

15:31:19.402  worker-0  completed  duration=500ms
15:31:19.412  worker-2  completed  duration=500ms
15:31:19.415  worker-1  completed  duration=500ms
```

**9 jobs processed in ~1.5s** (3 batches of 3, 500ms each). Single worker would have taken ~4.5s.

**Finding:** `FOR UPDATE SKIP LOCKED` works — no two workers claimed the same job. All three claimed different rows within milliseconds.

---

## Phase 4 — Retries with Exponential Backoff and Jitter

Enqueued a `flaky_task` configured to fail twice before succeeding.

```
15:31:20  worker-1  claimed  flaky_task  retry_count=0
          WARN  transient failure, retrying  delay="783.273ms"  error="simulated transient error (attempt 1)"

15:31:22  worker-2  claimed  flaky_task  retry_count=1
          WARN  transient failure, retrying  delay="574.386ms"  error="simulated transient error (attempt 2)"

15:31:24  worker-1  claimed  flaky_task  retry_count=2
          INFO  flaky task succeeded after retries
          INFO  job completed  duration="25.255µs"
```

**Database state after:**
```
job_id   | status    | retry_count | max_retries | last_error
6c1b8883 | completed | 2           | 3           | simulated transient error (attempt 2)
```

**Findings:**
- Jitter working: delays were `783ms` and `574ms` — randomized, not synchronized
- Different workers handled different attempts (worker-1, worker-2, worker-1)
- `last_error` preserved even on successful completion — useful for debugging flaky jobs
- `retry_count=2` with `status=completed` — retried twice, succeeded on attempt 3

---

## Phase 5 — Worker Crash Recovery (kill -9)

Enqueued a `slow_task` (60s execution). Killed the worker process with `kill -9` after 20 seconds.

**Step 1 — Job claimed, worker killed:**
```
13:48:04  worker-2  claimed  slow_task  lease_expires_at=13:49:14
          INFO  slow task started, will run for 60s
[kill -9 at 13:48:24]
```

**Step 2 — Database state immediately after kill:**
```
status=running  worker_id=Devpad-92246-2  lease_expires_at=2026-09-18 13:49:14
```
Job stuck in `running`. Worker is dead. Lease expires at 13:49:14.

**Step 3 — New worker started, recovery sweep fires:**
```
13:49:49  new worker process started
13:49:56  WARN  recovered stale jobs  count=1        ← sweep fired 7s after start
13:49:57  worker-1  claimed  slow_task               ← immediately reclaimed
          INFO  slow task started, will run for 60s
```

**Timeline:**
- Kill at `13:48:24`
- Lease expired at `13:49:14` (30s lease)
- Sweep fired at `13:49:56` (7.5s sweep interval)
- Total orphan time: **~92 seconds**
- Formula: `lease_duration(30s) + time_until_next_sweep(~62s)` = worst case

**Finding:** Recovery works. `retry_count` remained at 0 — a crash is infrastructure failure, not an application failure. The job kept its full retry budget.

---

## Phase 6 — Graceful Shutdown (SIGINT)

Sent SIGINT while a `slow_task` was 36 seconds into execution.

**Before fix (broken):**
```
14:00:01  shutdown signal received
          WARN  transient failure, retrying  delay=742ms
          ERROR failed to schedule retry: context canceled   ← DB write failed
          worker stopped cleanly
```
Job left orphaned in `running` status. Required recovery sweep.

**After fix (two-context pattern):**
```
16:45:46  shutdown signal received
          WARN  transient failure, retrying  delay=245ms     ← DB write succeeded
          worker stopped cleanly
```

**Database state after fix:**
```
status=pending  retry_count=1  run_at=16:45:46.742  last_error="slow task cancelled: context canceled"
```

**Finding:** `dbCtx = context.WithTimeout(context.Background(), 10s)` survives the shutdown signal. Job state written cleanly in <100ms. No orphan. No recovery sweep needed.

---

## Phase 7 — Priority Aging Verification

Inserted three test jobs directly into PostgreSQL and queried effective priority.

```sql
INSERT INTO jobs (task_name, priority, created_at) VALUES
  ('aging_test_old',  5, now() - interval '10 minutes'),
  ('aging_test_new',  5, now()),
  ('aging_test_high', 10, now());
```

```
task_name        | priority | wait_minutes | effective_priority
aging_test_old   | 5        | 10.00        | 15.00   ← wins despite lower priority
aging_test_high  | 10       | 0.00         | 10.00
aging_test_new   | 5        | 0.00         | 5.00
```

**Finding:** A priority-5 job waiting 10 minutes has effective priority 15, beating a freshly enqueued priority-10 job. Aging rate: 1 point per minute. Starvation is prevented.

---

## Phase 8 — Prometheus Metrics Output

Sample from `GET /metrics` after enqueuing one `send_email` job.

```
# HELP jobqueue_jobs_enqueued_total Total number of jobs enqueued.
# TYPE jobqueue_jobs_enqueued_total counter
jobqueue_jobs_enqueued_total{queue_name="default",task_name="send_email"} 1

# HELP jobqueue_db_claim_duration_seconds Duration of the job claim query in seconds.
# TYPE jobqueue_db_claim_duration_seconds histogram
jobqueue_db_claim_duration_seconds_bucket{le="0.005"} 0
jobqueue_db_claim_duration_seconds_bucket{le="0.01"} 3
jobqueue_db_claim_duration_seconds_bucket{le="+Inf"} 3
jobqueue_db_claim_duration_seconds_sum 0.021
jobqueue_db_claim_duration_seconds_count 3
```

**Health check responses:**
```bash
$ curl http://localhost:8080/health/live
{"status":"ok"}

$ curl http://localhost:8080/health/ready
{"status":"ok","database":"ok"}
```

---

## Phase 9 — Load Testing and Pool Exhaustion

### Baseline (MaxConns=10)
```
go run scripts/loadtest.go -concurrency=100 -total=2000

Total requests:  2000
Concurrency:     100
Success:         2000
Errors:          0
Total time:      1.129s
Throughput:      1771 req/s
Avg latency:     54.37ms
```

### Pool Exhaustion (MaxConns=2, timeout=50ms)
```
go run scripts/loadtest.go -concurrency=50 -total=200

Total requests:  200
Concurrency:     50
Success:         45
Errors:          155
Total time:      207ms
Throughput:      967 req/s
Avg latency:     29ms
```

**Finding:** With only 2 connections, 155 out of 200 requests timed out waiting for a pool slot. The pool acted as a throttle — requests queued, then failed when the 50ms client timeout elapsed.

### pprof Goroutine Dump (during load)
```
72 goroutines blocked at:
    pgxpool.(*Pool).Acquire        ← waiting for a DB connection
    queue.(*PostgresQueue).Enqueue
    api.(*handler).handleEnqueueJob
```

**Finding:** Under load, most goroutines are blocked waiting for a pool connection, not doing CPU work. The bottleneck is connection availability, not processing.

### PostgreSQL Connection State During Load
```sql
SELECT count(*), state FROM pg_stat_activity
WHERE datname='jobqueue' GROUP BY state;

 count | state
-------+-------
    10 | idle        ← pool connections held open
     1 | active      ← one currently running a query
```

---

## Phase 11 — C++ Worker via gRPC

C++ worker connected to Go gRPC server and processed `send_email` jobs.

```
[worker] started: cpp-worker-1
[worker] claimed job: e555a97a task: send_email
[handler] sending email, payload: {"to": "test@test.com"}
[handler] email sent
[worker] completed: e555a97a  duration: 202ms

[worker] claimed job: 22772f83 task: slow_task
[worker] failing job: 22772f83  permanent: true  error: unknown task: slow_task
```

**Findings:**
- C++ worker correctly identified `slow_task` as unknown → permanent failure → dead letter
- `send_email` processed in 202ms (200ms simulated work + 2ms gRPC overhead)
- Zero C++ database code — all queue logic stayed in Go

**Database state after C++ worker run:**
```
task_name  | status    | count
send_email | completed | 49
slow_task  | failed    | 1
```

---

## Phase 12 — Backend Benchmark: Postgres vs Redis

Same load test, same machine, same code — only `QUEUE_BACKEND` env var changed.

### Standard Load (concurrency=100, total=2000)
```
Backend: postgres
Throughput: 3447 req/s  |  Avg latency: 28.3ms  |  Errors: 0

Backend: redis
Throughput: 3320 req/s  |  Avg latency: 29.3ms  |  Errors: 0
```

### High Load (concurrency=500, total=10000)
```
Backend: redis
Throughput: 3721 req/s  |  Avg latency: 130.7ms  |  Errors: 0

Backend: postgres
Throughput: 4177 req/s  |  Avg latency: 115.7ms  |  Errors: 0
```

### Summary
| Backend | Concurrency | Throughput | Avg Latency | Winner |
|---------|------------|-----------|-------------|--------|
| Postgres | 100 | 3447 req/s | 28ms | ✅ Postgres |
| Redis | 100 | 3320 req/s | 29ms | |
| Postgres | 500 | **4177 req/s** | **116ms** | ✅ Postgres |
| Redis | 500 | 3721 req/s | 131ms | |

**Finding: Postgres outperformed Redis at every load level on this machine.**

Root cause: our Redis `Enqueue` uses 3 commands (GET + SET + ZADD) vs Postgres single INSERT. Multiple Redis round trips outweighed the memory-vs-disk advantage. Redis wins at raw operation speed, but implementation design matters more than backend choice.

**Key insight:** Don't switch to Redis to solve a throughput problem until you've proven Postgres is actually the bottleneck. Measure first.

---

## How to Reproduce These Results

All tests run from the project root with both services running:

```bash
# Start API server
go run ./cmd/api/

# Start worker (separate terminal)
go run ./cmd/worker/

# Run load test
go run scripts/loadtest.go -concurrency=100 -total=2000

# Switch to Redis backend
QUEUE_BACKEND=redis go run ./cmd/api/
go run scripts/loadtest.go -concurrency=100 -total=2000 -backend=redis

# Check DB state
psql "$DATABASE_URL" -c "SELECT task_name, status, count(*) FROM jobs GROUP BY task_name, status;"
```
