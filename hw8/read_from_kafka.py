import json
from cassandra_client import CassandraClient
from kafka import KafkaConsumer


def read_transactions():
    consumer = KafkaConsumer("transactions", bootstrap_servers='kafka-server:9092',
                             value_deserializer=lambda m: json.loads(m))

    for msg in consumer:
        dct = msg.value
        date = dct['transaction_date']

        print(dct)
        if dct["step"] != "step":

            step, tp, amount, name_org, old_balance_org, new_balance_org, \
                name_dest, old_balance_dest, new_balance_dest, is_fraud, is_flagged_fraud = int(dct["step"]), dct["type"], float(dct["amount"]), \
                dct["name_org"], float(dct["old_balance_org"]), float(dct["new_balance_org"]), dct["name_dest"], float(dct["old_balance_dest"]), \
                float(dct["new_balance_dest"]), int(
                    dct["is_fraud"]), int(dct["is_flagged_fraud"])

            client.insert_record(table1, date, step, tp, amount, name_org, old_balance_org, new_balance_org,
                                 name_dest, old_balance_dest, new_balance_dest, is_fraud, is_flagged_fraud)

            client.insert_record(table2, date, step, tp, amount, name_org, old_balance_org, new_balance_org,
                                 name_dest, old_balance_dest, new_balance_dest, is_fraud, is_flagged_fraud)

            client.insert_record(table3, date, step, tp, amount, name_org, old_balance_org, new_balance_org,
                                 name_dest, old_balance_dest, new_balance_dest, is_fraud, is_flagged_fraud)


if __name__ == "__main__":
    host = 'cassandra-node1'
    port = 9042
    keyspace = 'hw8'
    table1 = 'transactions_fraud'
    table2 = 'transactions_amount'
    table3 = 'transactions_date'
    client = CassandraClient(host, port, keyspace)

    client.connect()
    read_transactions()
    client.close()
