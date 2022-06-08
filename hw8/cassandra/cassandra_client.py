class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        from cassandra.cluster import Cluster
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def insert_record(self, table, date, step, tp, amount, name_org, old_balance_org, new_balance_org,
                      name_dest, old_balance_dest, new_balance_dest, is_fraud, is_flagged_fraud):
        query = f"INSERT INTO {table} (name_org, trans_date, step, tp, amount, old_balance_org, new_balance_org," + \
            "name_dest, old_balance_dest, new_balance_dest, is_fraud, is_flagged_fraud) VALUES (" + \
            f"'{name_org}', '{date}', {step}, '{tp}', {amount}, {old_balance_org}, {new_balance_org}, '{name_dest}'," + \
            f"{old_balance_dest}, {new_balance_dest}, {is_fraud}, {is_flagged_fraud})"
        self.execute(query)

    def query_one(self, uuid):
        query = f"SELECT * FROM transactions_fraud WHERE name_org='{uuid}' AND is_fraud=1;"
        rows = self.session.execute(query)
        return rows

    def query_two(self, uuid):
        query = f"SELECT * FROM transactions_amount WHERE name_org='{uuid}';"
        rows = self.session.execute(query)
        return rows

    def query_three(self, start_date, end_date, uuid):
        query = f"SELECT * FROM transactions_date WHERE name_org='{uuid}' AND trans_date > '{start_date}' AND trans_date < '{end_date}';"
        rows = self.session.execute(query)
        return rows
