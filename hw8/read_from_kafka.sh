docker build -t kafka_read:1.0 -f Dockerfile-read .
docker run --network general-network --rm kafka_read:1.0

#-v /Users/alexa/Desktop/big_data_homeworks/hw8:/opt/app