#!/bin/bash

rm -rf /tmp/mokv
rm -rf /tmp/mokv2
rm -rf /tmp/mokv3

# Start node 1 (bootstrap node) and wait for it to initialize
go run cmd/mokv/main.go --config-file=./example/config.yaml &
PID1=$!
echo "Started node 1 with PID $PID1"
sleep 5  # Wait 5 seconds for the bootstrap node to initialize

# Start node 2 and wait for it to join
go run cmd/mokv/main.go --config-file=./example/config2.yaml &
PID2=$!
echo "Started node 2 with PID $PID2"
sleep 3  # Wait 3 seconds for node 2 to join

# Start node 3
go run cmd/mokv/main.go --config-file=./example/config3.yaml &
PID3=$!
echo "Started node 3 with PID $PID3"

# Handle Ctrl+C before the wait
trap "kill $PID1 $PID2 $PID3; exit" SIGINT

# Wait for all processes
wait $PID1 $PID2 $PID3