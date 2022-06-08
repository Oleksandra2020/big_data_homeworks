FROM python:3.9-slim

WORKDIR /hw8

RUN apt-get update

RUN pip install --upgrade pip

RUN pip install cassandra-driver

RUN pip install flask-restful

RUN pip install flask

COPY app.py .

COPY read_from_kafka.py .

COPY ./cassandra/cassandra_client.py .

ENTRYPOINT ["python3", "app.py"]
