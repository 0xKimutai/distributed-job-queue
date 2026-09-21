//go:build ignore

// Load test script — run with: go run scripts/loadtest.go
// Sends N concurrent requests to POST /jobs and reports latency + errors.
//
// Usage:
//   go run scripts/loadtest.go -concurrency=50 -total=500 -url=http://localhost:8080

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	concurrency := flag.Int("concurrency", 50, "number of concurrent workers")
	total := flag.Int("total", 500, "total number of requests")
	url := flag.String("url", "http://localhost:8080", "base URL")
	backend := flag.String("backend", "postgres", "queue backend label (for display only)")
	flag.Parse()

	fmt.Printf("Backend: %s | Concurrency: %d | Total: %d\n", *backend, *concurrency, *total)

	var (
		success   atomic.Int64
		errors    atomic.Int64
		totalTime atomic.Int64 // nanoseconds
	)

	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup

	start := time.Now()

	for i := 0; i < *total; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()

			body, _ := json.Marshal(map[string]any{
				"task_name": "send_email",
				"payload":   map[string]any{"i": i},
				"priority":  i % 10,
			})

			reqStart := time.Now()
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Post(*url+"/jobs", "application/json", bytes.NewReader(body))
			elapsed := time.Since(reqStart)

			if err != nil {
				errors.Add(1)
				fmt.Printf("ERROR: %v\n", err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode == http.StatusCreated {
				success.Add(1)
				totalTime.Add(elapsed.Nanoseconds())
			} else {
				errors.Add(1)
				fmt.Printf("HTTP %d\n", resp.StatusCode)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	s := success.Load()
	e := errors.Load()
	t := totalTime.Load()

	fmt.Printf("\n=== Load Test Results ===\n")
	fmt.Printf("Total requests:  %d\n", *total)
	fmt.Printf("Concurrency:     %d\n", *concurrency)
	fmt.Printf("Success:         %d\n", s)
	fmt.Printf("Errors:          %d\n", e)
	fmt.Printf("Total time:      %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Throughput:      %.0f req/s\n", float64(*total)/elapsed.Seconds())
	if s > 0 {
		fmt.Printf("Avg latency:     %s\n", time.Duration(t/s).Round(time.Microsecond))
	}
}
