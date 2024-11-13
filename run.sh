#!/bin/bash

# List of commands to be executed
commands=(
	"./cockroach start --insecure --store=node1 --listen-addr=localhost:26257 --http-addr=localhost:8080 --join=localhost:26257,localhost:26258,localhost:26259 --background &"
	"./cockroach start --insecure --store=node2 --listen-addr=localhost:26258 --http-addr=localhost:8081 --join=localhost:26257,localhost:26258,localhost:26259 --background &"
	"./cockroach start --insecure --store=node3 --listen-addr=localhost:26259 --http-addr=localhost:8082 --join=localhost:26257,localhost:26258,localhost:26259 --background &"
	"./cockroach init --insecure --host=localhost:26257 &"
	"./cockroach workload init ycsbt 'postgresql://root@localhost:26257?sslmode=disable' &"
	"./cockroach workload run ycsbt --duration=1m 'postgresql://root@localhost:26257?sslmode=disable' &"
)

# Loop through each command
for cmd in "${commands[@]}"; do
	echo "Executing: $cmd"
	# Run the command in the background
	eval "$cmd"
	# Wait for the command to finish
	wait
done
