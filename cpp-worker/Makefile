CXX      := g++
CXXFLAGS := -std=c++17 -Wall -O2
LDFLAGS  := $(shell pkg-config --libs grpc++ protobuf)
INCLUDES := $(shell pkg-config --cflags grpc++ protobuf)

PROTO_SRCS := proto/jobqueue.pb.cc proto/jobqueue.grpc.pb.cc
WORKER_SRCS := worker.cpp

TARGET := jobqueue-cpp-worker

.PHONY: all clean

all: $(TARGET)

$(TARGET): $(WORKER_SRCS) $(PROTO_SRCS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)
