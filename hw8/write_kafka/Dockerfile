FROM python:3.9-slim

WORKDIR /hw8

RUN apt-get update

RUN pip install --upgrade pip

RUN pip install kafka-python

COPY write_to_kafka.py .

COPY PS_20174392719_1491204439457_log.csv .

ENTRYPOINT ["python3", "write_to_kafka.py"]