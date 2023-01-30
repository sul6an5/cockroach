ded# Experiment Settings and Deployments


Second, Run CRDB on the Cluster. 

cockroach start --insecure --store=tpcc-local1 --listen-addr=localhost:26257 --http-addr=localhost:8080 --join=localhost:26257,localhost:26258,localhost:26259 --background

cockroach start --insecure --store=tpcc-local2 --listen-addr=localhost:26258 --http-addr=localhost:8081 --join=localhost:26257,localhost:26258,localhost:26259 --background

cockroach start --insecure --store=tpcc-local3 --listen-addr=localhost:26259 --http-addr=localhost:8082 --join=localhost:26257,localhost:26258,localhost:26259 --background

cockroach init --insecure --host=localhost:26257

cockroach workload fixtures import tpcc --warehouses=10 'postgresql://root@localhost:26257?sslmode=disable'


cockroach workload run tpcc --warehouses=10 --ramp=3m --duration=1m 'postgresql://root@localhost:26257?sslmode=disable'



