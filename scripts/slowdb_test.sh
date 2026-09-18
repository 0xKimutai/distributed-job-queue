#!/bin/bash
# Simulates slow DB by locking a connection with pg_sleep in one terminal
# while running the load test in another.
#
# Run this in terminal 1:
#   bash scripts/slowdb_test.sh
#
# Then immediately run in terminal 2:
#   go run scripts/loadtest.go -concurrency=20 -total=100

echo "Holding 8 DB connections for 10 seconds (simulating slow queries)..."
for i in $(seq 1 8); do
  PGPASSWORD='jobqueue_secret' psql -U jobqueue -d jobqueue \
    -c "SELECT pg_sleep(10);" &
done

echo "Connections held. Run the load test now in another terminal."
wait
echo "Done."
