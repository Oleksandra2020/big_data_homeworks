docker build . -t kafka_write:1.0
docker run --network general-network --rm kafka_write:1.0
