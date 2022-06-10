docker build -f Dockerfile-app . -t run_app:1.0
docker run -p 8080:8080 --network my-cassandra-network --rm run_app:1.0
