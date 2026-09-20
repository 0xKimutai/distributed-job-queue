package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/distributed-job-queue/internal/backoff"
	pb "github.com/distributed-job-queue/internal/grpc/pb"
	"github.com/distributed-job-queue/internal/queue"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// JobQueueServer implements the gRPC JobQueueServiceServer interface.
// It wraps our existing queue.Queue — the same logic used by Go workers
// is now exposed to workers in any language via gRPC.
//
// This is the key architectural point: the C++ worker doesn't touch
// PostgreSQL directly. All queue semantics (atomic claiming, lease
// management, retry logic) live here in Go and are called via RPC.
type JobQueueServer struct {
	// Embed the unimplemented server to satisfy the interface for any
	// methods we haven't implemented yet — protoc-gen-go-grpc requires this.
	pb.UnimplementedJobQueueServiceServer
	queue *queue.Queue
}

func New(q *queue.Queue) *JobQueueServer {
	return &JobQueueServer{queue: q}
}

// ClaimJob atomically claims the next available job and returns it.
// Returns gRPC NOT_FOUND if the queue is empty — the C++ worker
// should sleep and retry rather than treating this as an error.
func (s *JobQueueServer) ClaimJob(ctx context.Context, req *pb.ClaimJobRequest) (*pb.ClaimJobResponse, error) {
	workerID := req.WorkerId
	if workerID == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	queueName := req.QueueName
	if queueName == "" {
		queueName = "default"
	}

	leaseDuration := time.Duration(req.LeaseSeconds) * time.Second
	if leaseDuration <= 0 {
		leaseDuration = 30 * time.Second
	}

	job, err := s.queue.Claim(ctx, workerID, leaseDuration)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "no jobs available")
		}
		slog.Error("grpc: failed to claim job", "worker_id", workerID, "error", err)
		return nil, status.Error(codes.Internal, "failed to claim job")
	}

	slog.Info("grpc: job claimed", "worker_id", workerID, "job_id", job.JobID, "task", job.TaskName)

	return &pb.ClaimJobResponse{
		JobId:      job.JobID.String(),
		TaskName:   job.TaskName,
		Payload:    job.Payload,
		Priority:   int32(job.Priority),
		RetryCount: int32(job.RetryCount),
		MaxRetries: int32(job.MaxRetries),
	}, nil
}

// CompleteJob marks a job as successfully finished.
func (s *JobQueueServer) CompleteJob(ctx context.Context, req *pb.CompleteJobRequest) (*pb.JobAck, error) {
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}

	if err := s.queue.Complete(ctx, req.JobId); err != nil {
		slog.Error("grpc: failed to complete job", "job_id", req.JobId, "error", err)
		return nil, status.Error(codes.Internal, "failed to complete job")
	}

	slog.Info("grpc: job completed", "job_id", req.JobId, "worker_id", req.WorkerId)
	return &pb.JobAck{JobId: req.JobId}, nil
}

// FailJob marks a job failed. If permanent=true or retries exhausted,
// sends to dead letter. Otherwise schedules retry with backoff.
func (s *JobQueueServer) FailJob(ctx context.Context, req *pb.FailJobRequest) (*pb.JobAck, error) {
	if req.JobId == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}

	jobErr := fmt.Errorf("%s", req.ErrorMsg)

	// Fetch current retry state to decide retry vs dead letter.
	// We need retry_count and max_retries — get them from the DB.
	// For simplicity we pass retry info through the request in Phase 11.
	// A production system would look this up server-side.
	if req.Permanent {
		slog.Error("grpc: permanent job failure", "job_id", req.JobId, "error", req.ErrorMsg)
		if err := s.queue.Fail(ctx, req.JobId, backoff.Permanent(jobErr)); err != nil {
			return nil, status.Error(codes.Internal, "failed to record permanent failure")
		}
		return &pb.JobAck{JobId: req.JobId}, nil
	}

	// Transient failure — schedule retry with backoff.
	// retry_count comes from the ClaimJobResponse the worker received.
	delay := backoff.Calculate(int(req.RetryCount), backoff.DefaultBase, backoff.DefaultMaxDelay)
	slog.Warn("grpc: transient job failure, retrying",
		"job_id", req.JobId,
		"retry_count", req.RetryCount,
		"delay", delay.String(),
		"error", req.ErrorMsg,
	)

	if err := s.queue.Retry(ctx, req.JobId, jobErr, delay); err != nil {
		return nil, status.Error(codes.Internal, "failed to schedule retry")
	}

	return &pb.JobAck{JobId: req.JobId}, nil
}
