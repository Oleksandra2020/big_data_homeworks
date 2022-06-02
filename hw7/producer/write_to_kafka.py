import csv
import json
import datetime
import time

from kafka import KafkaProducer


def send_tweets():
    producer = KafkaProducer(bootstrap_servers='kafka-server:9092',
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    with open('twcs.csv') as f:
        csvfile = csv.reader(f)
        for row in csvfile:
            producer.send("tweets", {"created_at": str(
                datetime.datetime.now()), "text": row[4], "author_id": row[1]})

            time.sleep(0.1)
    producer.flush()


if __name__ == "__main__":
    send_tweets()
