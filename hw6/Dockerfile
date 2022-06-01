FROM python:3.9-slim

RUN apt-get update

RUN pip install --upgrade pip

RUN pip install kafka-python

COPY ./write_to_kafka.py .
COPY ./twcs.csv .

ENTRYPOINT ["python3", "write_to_kafka.py"]