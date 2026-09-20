#!/bin/bash
# Regenerate gRPC/protobuf code from proto/jobqueue.proto.
# Run from project root: bash scripts/gen_proto.sh
#
# Requires:
#   protoc         (apt install protobuf-compiler)
#   protoc-gen-go  (go install google.golang.org/protobuf/cmd/protoc-gen-go@latest)
#   protoc-gen-go-grpc (go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest)

set -euo pipefail

export PATH="$PATH:$(go env GOPATH)/bin"

echo "Generating Go stubs..."
protoc \
  --proto_path=proto \
  --go_out=internal/grpc/pb \
  --go_opt=paths=source_relative \
  --go-grpc_out=internal/grpc/pb \
  --go-grpc_opt=paths=source_relative \
  proto/jobqueue.proto

echo "Generating C++ stubs..."
mkdir -p cpp-worker/proto
protoc \
  --proto_path=proto \
  --cpp_out=cpp-worker/proto \
  --grpc_out=cpp-worker/proto \
  --plugin=protoc-gen-grpc=$(which grpc_cpp_plugin 2>/dev/null || echo "grpc_cpp_plugin not found") \
  proto/jobqueue.proto 2>/dev/null || echo "C++ plugin not available — install libgrpc++-dev"

echo "Done."
