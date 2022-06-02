FROM python:3.9-slim

WORKDIR /hw7

RUN apt-get update

RUN pip install --upgrade pip

RUN pip install kafka-python

COPY ./read_from_kafka.py .

COPY ./files .

ENTRYPOINT ["python3", "read_from_kafka.py"]