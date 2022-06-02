import csv
import json
import datetime
import ast

from kafka import KafkaConsumer


def write_tweets():
    consumer = KafkaConsumer("tweets", bootstrap_servers='kafka-server:9092',
                             value_deserializer=lambda m: json.loads(m))

    cur_date = None
    fields = ["author_id", "created_at", "text"]
    rows = []

    for msg in consumer:
        dct = msg.value
        date = datetime.datetime.strptime(
            dct['created_at'].split(".")[0], '%Y-%m-%d %H:%M:%S')

        if cur_date is None:
            cur_date = date

        elif date.year == cur_date.year and \
                date.month == cur_date.month and \
                date.day == cur_date.day and \
                date.hour == cur_date.hour:

            if date.minute != cur_date.minute:
                print(date.minute, cur_date.minute, cur_date.day,
                      cur_date.month, cur_date.year, cur_date.hour)
                with open(f"./files/tweets_{cur_date.day}_{cur_date.month}_{cur_date.year}_{cur_date.hour}_{cur_date.minute}.csv",
                          'w+', encoding='utf-8') as csvfile:
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(fields)
                    csvwriter.writerows(rows)
                cur_date = date
                rows = []

            text = dct['text']
            created_at = dct['created_at']
            author = dct['author_id']
            rows.append([author, created_at, text])


if __name__ == "__main__":
    write_tweets()
