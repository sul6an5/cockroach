#!/bin/bash
# Test YCSB+T flags with a cluster of 2 servers 
# and 1 client, each client has 8 concurrent workers/threads

# Run this script in the current directory, i.e., 

# -------- Specify flag values ------------
ycsb_n_keys=100000       # total number of rows, default: 10000. Note: crdb's rmw has bugs when rows<10K
                        # the bugs are in the QueryRow method, reporting error "no rows in result set", 
                        # due to attempting to read a non-existing key. Investigate it later.
distribution=zipfian    # zifpian or uniform, default: zipfian
duration=30s            # run time, default: 1 min
tx_size=10              # txn size, default: 10 requests per txn
zipf=0.99               # zipfian constant, default: 0.99
write_fraction=0.5      # write fraction, default: 10%
rmw=0.0                 # fraction of read-modify-write in write txns, default: 0 (all writes are updates)
parallel=ONERW          # whether requests are sent in "ONE", "TWO", "ONERW" or "MULTI" shots
                        # ONE: only has read-only (SELECT IN) and write-only (UPDATE IN) txs
                        # TWO: has read-only (SELECT IN), write-only (UPDATE IN), and two-shot (SELECT IN, then UPDATE IN) txs
                        # ONERW: has read-only (SELECT IN), write-only (UPDATE IN), and one-shot batched (SELECT, UPDATE) txs -- all are one-shot
                        # MULTISHOT: all requests are sent in serial
n_workers_per_client=16  # client threads 
# -----------------------------------------

executable=../../../../cockroach  # this should be pointing to the executable ...../cockroachdb/cockroach/cockroach
expt_id=$(date +%s)
log_file="${expt_id}.log"
result_file="${expt_id}.res"

commands=(
    # Clean environment first
    "for pid in $(ps -ef | grep ycsb | grep -v grep | awk '{ print $2 }'); do kill -KILL ${pid}; done"
    "for pid in $(ps -ef | grep cockroach | grep -v grep | awk '{ print $2 }'); do kill -KILL ${pid}; done"  
    "rm -rf node1 node2 node3 node4"
    # Start cluster
    "${executable} start --insecure --store=node1 --listen-addr=localhost:26257 --http-addr=localhost:8080 --join=localhost:26257,localhost:26258,localhost:26259,localhost:26260 --background &"
    "${executable} start --insecure --store=node2 --listen-addr=localhost:26258 --http-addr=localhost:8081 --join=localhost:26257,localhost:26258,localhost:26259,localhost:26260 --background &"
    "${executable} start --insecure --store=node3 --listen-addr=localhost:26259 --http-addr=localhost:8082 --join=localhost:26257,localhost:26258,localhost:26259,localhost:26260 --background &"
    "${executable} start --insecure --store=node4 --listen-addr=localhost:26260 --http-addr=localhost:8083 --join=localhost:26257,localhost:26258,localhost:26259,localhost:26260 --background &"
    # Initialize cluster
    "${executable} init --insecure --host=localhost:26257 &"
    # Initialize YCSB+T
    "${executable} workload init ycsbt --drop --insert-count=${ycsb_n_keys} 'postgresql://root@localhost:26257?sslmode=disable' &"
    # Run YCSB+T
    "${executable} workload run ycsbt --insert-count=0 --record-count=${ycsb_n_keys} --request-distribution=${distribution} --concurrency=${n_workers_per_client} --parallel=${parallel} --tx-size=${tx_size} --duration=${duration} --zipf=${zipf} --write-fraction=${write_fraction} --rmw=${rmw} 'postgresql://root@localhost:26257?sslmode=disable' >> ${log_file}"
    # "${executable} workload run ycsb --insert-count=0 ${ycsb_n_keys} --request-distribution=uniform --concurrency=16 --duration=${duration} 'postgresql://root@localhost:26257?sslmode=disable' >> ${log_file}"
)

for command in "${commands[@]}"; do
  echo "Executing: ${command}"
  # Run the command in the background
  eval "${command}"
  # Wait for the command to finish
  wait
done 

# Get results
touch ${result_file}
echo -e "Total_txns \t Throughput_(txn/sec) \t Mean_Latency_(ms) \t Median_Latency_(ms) \t p99_Latency_(ms)" >> ${result_file}
grep -v '^\s*$' ${log_file} | tail -n 1 | awk -F " " {'print $3 " \t " $4 " \t " $5 " \t " $6 " \t " $8'} >> ${result_file}
cat ${result_file} | awk '{printf "%-25s %-25s %-25s %-25s %-25s\n", $1, $2, $3, $4, $5}' > tmp
rm ${result_file}
mv tmp ${result_file}

# Kill cluster
for pid in $(ps -ef | grep ycsb | grep -v grep | awk '{ print $2 }'); do
        kill -KILL ${pid}
done

for pid in $(ps -ef | grep cockroach | grep -v grep | awk '{ print $2 }'); do
        kill -KILL ${pid}
done

rm -rf node1 node2 node3 node4

echo "========================================================================="
echo "Test Run Finished. See results in ${result_file}, and log in ${log_file}."
