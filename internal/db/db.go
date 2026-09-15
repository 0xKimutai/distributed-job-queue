package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Connect creates and validates a PostgreSQL connection pool.
//
// We use pgxpool rather than a single connection because:
//   - The API server and workers handle multiple concurrent operations
//   - A pool keeps connections warm, avoiding TCP + TLS handshake overhead per query
//   - pgxpool is safe for concurrent use; a single *pgx.Conn is not
//
// The pool is configured conservatively here. We'll tune these numbers
// in Phase 9 when we benchmark and deliberately exhaust connections.
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing database URL: %w", err)
	}

	// Maximum number of connections this pool will open.
	// With max_connections=100 on Postgres and potentially multiple
	// processes (API + N workers), you must budget carefully.
	// Rule of thumb: (max_pg_connections - 3 superuser slots) / num_processes
	cfg.MaxConns = 10

	// How long a connection can sit idle before being closed.
	// Keeps the pool from holding connections that Postgres has already
	// timed out on its side — which causes "connection reset by peer" errors.
	cfg.MaxConnIdleTime = 5 * time.Minute

	// How long to wait for a connection from the pool before giving up.
	// Without this, a burst of requests during DB slowness queues forever.
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("creating connection pool: %w", err)
	}

	// Eagerly verify the connection works. Fail fast at startup.
	// A pool.NewWithConfig does NOT open connections immediately — it's lazy.
	// Ping forces at least one connection attempt so we catch bad credentials
	// or unreachable hosts before the server starts accepting requests.
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pinging database: %w", err)
	}

	return pool, nil
}
