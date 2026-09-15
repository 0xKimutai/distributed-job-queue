package db

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// RunMigrations applies pending SQL migration files from migrationsPath.
//
// Rather than using golang-migrate (which requires registering a database
// driver via a blank import), we implement a minimal migration runner on
// top of pgx directly. This is a common production pattern — the logic is
// simple enough that the dependency isn't worth the complexity.
//
// How it works:
//  1. Create a schema_migrations table if it doesn't exist.
//  2. Read all *.up.sql files from the migrations directory, sorted by name.
//  3. For each file, check whether it has already been applied.
//  4. If not, execute it inside a transaction and record it in schema_migrations.
//
// Each migration runs in its own transaction so a failure leaves the
// database in a known state (either fully applied or not at all).
func RunMigrations(ctx context.Context, databaseURL, migrationsPath string) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connecting for migrations: %w", err)
	}
	defer conn.Close(ctx)

	// Ensure the tracking table exists.
	_, err = conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     TEXT PRIMARY KEY,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	if err != nil {
		return fmt.Errorf("creating schema_migrations table: %w", err)
	}

	// Find all up-migration files, sorted lexicographically.
	// The numeric prefix (001_, 002_, …) ensures correct order.
	pattern := filepath.Join(migrationsPath, "*.up.sql")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("globbing migration files: %w", err)
	}
	sort.Strings(files)

	if len(files) == 0 {
		slog.Warn("no migration files found", "path", migrationsPath)
		return nil
	}

	applied := 0
	for _, file := range files {
		version := filepath.Base(file) // e.g. "001_create_jobs.up.sql"

		// Check whether this version was already applied.
		var exists bool
		err := conn.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`,
			version,
		).Scan(&exists)
		if err != nil {
			return fmt.Errorf("checking migration %s: %w", version, err)
		}
		if exists {
			slog.Debug("migration already applied, skipping", "version", version)
			continue
		}

		// Read the SQL file.
		sql, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("reading migration file %s: %w", file, err)
		}

		// Execute inside a transaction — all-or-nothing.
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("beginning transaction for %s: %w", version, err)
		}

		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("executing migration %s: %w", version, err)
		}

		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, version,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("recording migration %s: %w", version, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("committing migration %s: %w", version, err)
		}

		slog.Info("migration applied", "version", strings.TrimSuffix(version, ".up.sql"))
		applied++
	}

	if applied == 0 {
		slog.Info("database schema is up to date, no migrations to run")
	} else {
		slog.Info("migrations complete", "applied", applied)
	}

	return nil
}
