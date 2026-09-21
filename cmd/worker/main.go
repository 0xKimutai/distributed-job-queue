package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/distributed-job-queue/config"
	"github.com/distributed-job-queue/internal/db"
	"github.com/distributed-job-queue/internal/queue"
	"github.com/distributed-job-queue/internal/worker"
	goredis "github.com/redis/go-redis/v9"
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

	// Select queue backend from config — same logic as cmd/api/main.go.
	var q queue.Queue
	switch cfg.QueueBackend {
	case "redis":
		opt, err := goredis.ParseURL(cfg.RedisURL)
		if err != nil {
			slog.Error("invalid REDIS_URL", "error", err)
			os.Exit(1)
		}
		q = queue.NewRedisQueue(goredis.NewClient(opt))
		slog.Info("using Redis queue backend", "url", cfg.RedisURL)
	default:
		q = queue.New(pool)
		slog.Info("using Postgres queue backend")
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		slog.Info("shutdown signal received")
		cancel()
	}()

	var wg sync.WaitGroup

	sweeper := worker.New(fmt.Sprintf("%s-sweeper", cfg.WorkerID), q, cfg)
	wg.Add(1)
	go func() {
		defer wg.Done()
		sweeper.RunRecoverySweep(ctx)
	}()

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

	slog.Info("worker stopped cleanly")
}
