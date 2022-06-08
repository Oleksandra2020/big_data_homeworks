docker build -f Dockerfile . -t run_app:1.0
docker run -p 8080:8080 --network general-network --rm run_app:1.0
