import csv
import json
import datetime
import time
import random

from kafka import KafkaProducer


def write_to_kafka():
    producer = KafkaProducer(bootstrap_servers='kafka-server:9092',
                             value_serializer=lambda m: json.dumps(m).encode('utf-8'))

    with open('PS_20174392719_1491204439457_log.csv') as f:
        csvfile = csv.reader(f)
        for row in csvfile:
            now = datetime.datetime.now()
            random_day = random.choice(
                [(now - datetime.timedelta(days=day)).strftime("%Y-%m-%d") for day in range(30)])
            producer.send("transactions", {"transaction_date": str(
                random_day), "step": row[0], "type": row[1], "amount": row[2], "name_org": row[3],
                "old_balance_org": row[4], "new_balance_org": row[5], "name_dest": row[6],
                "old_balance_dest": row[7], "new_balance_dest": row[8], "is_fraud": row[9],
                "is_flagged_fraud": row[10]})
            time.sleep(0.05)
    producer.flush()


if __name__ == "__main__":
    write_to_kafka()
