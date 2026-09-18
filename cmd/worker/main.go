package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"fmt"
	"sync"

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

	// Graceful shutdown on SIGINT/SIGTERM.
	// Cancelling ctx causes worker.Run() to exit cleanly after its current job.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		slog.Info("shutdown signal received")
		cancel()
	}()

	var wg sync.WaitGroup

	// One dedicated recovery sweep goroutine — resets orphaned jobs whose
	// lease expired (i.e. the worker that claimed them crashed or stalled).
	sweeper := worker.New(fmt.Sprintf("%s-sweeper", cfg.WorkerID), q, cfg)
	wg.Add(1)
	go func() {
		defer wg.Done()
		sweeper.RunRecoverySweep(ctx)
	}()

	// N poll-loop worker goroutines — each independently claims and executes jobs.
	for i := 0; i < cfg.WorkerConcurrency; i++ {
		workerID := fmt.Sprintf("%s-%d", cfg.WorkerID, i)
		w := worker.New(workerID, q, cfg)
		wg.Add(1)
		go func(w *worker.Worker) {
			defer wg.Done()
			w.Run(ctx)
		}(w)
	}
	wg.Wait()
	wg.Wait()


	slog.Info("worker stopped cleanly")
}
