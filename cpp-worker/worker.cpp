#include <chrono>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>
#include "proto/jobqueue.grpc.pb.h"

// JobWorker connects to the Go gRPC server and processes jobs.
//
// This mirrors the Go worker's poll loop:
//   1. Call ClaimJob — get a job or get NOT_FOUND (sleep and retry)
//   2. Dispatch to a handler based on task_name
//   3. Call CompleteJob or FailJob based on the result
//
// The C++ worker doesn't touch PostgreSQL directly.
// All queue semantics live in the Go gRPC server.
class JobWorker {
public:
    JobWorker(const std::string& worker_id,
              const std::string& server_address,
              std::chrono::seconds poll_interval = std::chrono::seconds(2))
        : worker_id_(worker_id),
          poll_interval_(poll_interval) {

        // Create a gRPC channel to the Go server.
        // InsecureChannelCredentials — no TLS for local dev.
        // In production: use SslCredentials with a certificate.
        auto channel = grpc::CreateChannel(
            server_address,
            grpc::InsecureChannelCredentials()
        );
        stub_ = jobqueue::JobQueueService::NewStub(channel);

        std::cout << "[worker] started: " << worker_id_ << "\n";
    }

    // Run polls for jobs until stopped. Blocks until stop() is called.
    void Run() {
        running_ = true;
        while (running_) {
            PollOnce();
            std::this_thread::sleep_for(poll_interval_);
        }
        std::cout << "[worker] stopped: " << worker_id_ << "\n";
    }

    void Stop() { running_ = false; }

private:
    void PollOnce() {
        grpc::ClientContext ctx;
        // 5-second deadline on the RPC — don't hang forever if the server is down.
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

        jobqueue::ClaimJobRequest req;
        req.set_worker_id(worker_id_);
        req.set_queue_name("default");
        req.set_lease_seconds(30);

        jobqueue::ClaimJobResponse resp;
        grpc::Status claim_status = stub_->ClaimJob(&ctx, req, &resp);

        if (!claim_status.ok()) {
            if (claim_status.error_code() == grpc::StatusCode::NOT_FOUND) {
                // No jobs available — expected quiet path.
                return;
            }
            std::cerr << "[worker] claim error: "
                      << claim_status.error_message() << "\n";
            return;
        }

        std::cout << "[worker] claimed job: " << resp.job_id()
                  << " task: " << resp.task_name() << "\n";

        ExecuteJob(resp);
    }

    void ExecuteJob(const jobqueue::ClaimJobResponse& job) {
        auto start = std::chrono::steady_clock::now();

        // Dispatch to handler based on task_name.
        bool permanent_failure = false;
        std::string error_msg;
        bool success = false;

        try {
            if (job.task_name() == "send_email") {
                success = HandleSendEmail(job);
            } else if (job.task_name() == "resize_image") {
                success = HandleResizeImage(job);
            } else {
                // Unknown task — permanent failure, don't retry.
                permanent_failure = true;
                error_msg = "unknown task: " + job.task_name();
                success = false;
            }
        } catch (const std::exception& e) {
            error_msg = std::string("exception: ") + e.what();
            success = false;
        }

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start
        );

        if (success) {
            CompleteJob(job.job_id());
            std::cout << "[worker] completed: " << job.job_id()
                      << " duration: " << elapsed.count() << "ms\n";
        } else {
            FailJob(job.job_id(), error_msg, permanent_failure,
                    job.retry_count(), job.max_retries());
        }
    }

    // CompleteJob notifies the Go server that the job succeeded.
    void CompleteJob(const std::string& job_id) {
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));

        jobqueue::CompleteJobRequest req;
        req.set_job_id(job_id);
        req.set_worker_id(worker_id_);

        jobqueue::JobAck ack;
        grpc::Status s = stub_->CompleteJob(&ctx, req, &ack);
        if (!s.ok()) {
            std::cerr << "[worker] complete error for " << job_id
                      << ": " << s.error_message() << "\n";
        }
    }

    // FailJob notifies the Go server that the job failed.
    // The Go server decides whether to retry or dead-letter based on
    // retry_count, max_retries, and the permanent flag.
    void FailJob(const std::string& job_id,
                 const std::string& error_msg,
                 bool permanent,
                 int32_t retry_count,
                 int32_t max_retries) {

        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));

        jobqueue::FailJobRequest req;
        req.set_job_id(job_id);
        req.set_worker_id(worker_id_);
        req.set_error_msg(error_msg);
        req.set_permanent(permanent || retry_count >= max_retries);
        req.set_retry_count(retry_count);

        std::cout << "[worker] failing job: " << job_id
                  << " permanent: " << (req.permanent() ? "true" : "false")
                  << " error: " << error_msg << "\n";

        jobqueue::JobAck ack;
        grpc::Status s = stub_->FailJob(&ctx, req, &ack);
        if (!s.ok()) {
            std::cerr << "[worker] fail error for " << job_id
                      << ": " << s.error_message() << "\n";
        }
    }

    // ── Job Handlers ──────────────────────────────────────────────────────────

    bool HandleSendEmail(const jobqueue::ClaimJobResponse& job) {
        // Payload is raw JSON bytes — parse with your JSON library of choice.
        // For this demo, we just log it.
        std::string payload(job.payload().begin(), job.payload().end());
        std::cout << "[handler] sending email, payload: " << payload << "\n";

        // Simulate work.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        std::cout << "[handler] email sent\n";
        return true; // success
    }

    bool HandleResizeImage(const jobqueue::ClaimJobResponse& job) {
        std::string payload(job.payload().begin(), job.payload().end());
        std::cout << "[handler] resizing image, payload: " << payload << "\n";
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        std::cout << "[handler] image resized\n";
        return true;
    }

    std::string worker_id_;
    std::chrono::seconds poll_interval_;
    std::atomic<bool> running_{false};
    std::unique_ptr<jobqueue::JobQueueService::Stub> stub_;
};

int main(int argc, char* argv[]) {
    std::string server_addr = "localhost:50051";
    std::string worker_id   = "cpp-worker-1";

    if (argc > 1) server_addr = argv[1];
    if (argc > 2) worker_id   = argv[2];

    JobWorker worker(worker_id, server_addr);
    worker.Run();

    return 0;
}
