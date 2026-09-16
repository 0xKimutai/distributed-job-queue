package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/distributed-job-queue/config"
	"github.com/distributed-job-queue/internal/db"
	"github.com/distributed-job-queue/internal/queue"
	"github.com/distributed-job-queue/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	q := queue.New(pool)
	w := worker.New(cfg.WorkerID, q, cfg)

	// Graceful shutdown on SIGINT/SIGTERM.
	// Cancelling ctx causes worker.Run() to exit cleanly after its current job.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		slog.Info("shutdown signal received")
		cancel()
	}()

	// Blocks until ctx is cancelled.
	w.Run(ctx)
	slog.Info("worker stopped cleanly")
}
