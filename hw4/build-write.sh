docker build -f Dockerfile-write . -t run_write:1.0
docker run --network my-cassandra-network --rm run_write:1.0
