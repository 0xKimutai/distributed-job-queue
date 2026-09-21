package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/distributed-job-queue/config"
	"github.com/distributed-job-queue/internal/api"
	"github.com/distributed-job-queue/internal/db"
	pb "github.com/distributed-job-queue/internal/grpc/pb"
	grpcserver "github.com/distributed-job-queue/internal/grpc/server"
	"github.com/distributed-job-queue/internal/queue"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	if err := db.RunMigrations(ctx, cfg.DatabaseURL, cfg.MigrationsPath); err != nil {
		slog.Error("failed to run migrations", "error", err)
		os.Exit(1)
	}

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Select queue backend based on QUEUE_BACKEND env var.
	// Both implement queue.Queue — everything downstream is backend-agnostic.
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

	// ── HTTP server ───────────────────────────────────────────────────────────
	router := api.NewRouter(pool, cfg)
	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.APIPort),
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		slog.Info("HTTP server starting", "port", cfg.APIPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	// ── gRPC server ───────────────────────────────────────────────────────────
	// Listens on a separate port from the HTTP API.
	// C++ workers connect here; HTTP clients use the REST API.
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.GRPCPort))
	if err != nil {
		slog.Error("failed to listen on gRPC port", "port", cfg.GRPCPort, "error", err)
		os.Exit(1)
	}

	grpcSrv := grpc.NewServer()
	pb.RegisterJobQueueServiceServer(grpcSrv, grpcserver.New(q))

	go func() {
		slog.Info("gRPC server starting", "port", cfg.GRPCPort)
		if err := grpcSrv.Serve(grpcListener); err != nil {
			slog.Error("gRPC server error", "error", err)
			os.Exit(1)
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutdown signal received, draining...")

	// Stop gRPC gracefully — waits for in-flight RPCs to complete.
	grpcSrv.GracefulStop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("forced HTTP shutdown", "error", err)
	}

	slog.Info("server stopped cleanly")
}
