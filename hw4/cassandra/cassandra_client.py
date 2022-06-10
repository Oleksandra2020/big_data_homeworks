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

    def insert_record(self, table, customer_id, review_id, product_id, star_rating, review_date, verified_purchase):
        query = f"INSERT INTO {table} (customer_id, review_id, product_id, star_rating, review_date, verified_purchase) VALUES (" + \
            f"'{customer_id}', '{review_id}', '{product_id}', {star_rating}, '{review_date}', '{verified_purchase}')"
        self.execute(query)

    def query_one(self, product_id):
        query = f"SELECT review_id FROM reviews_by_product WHERE product_id='{product_id}' AND star_rating > -1;"
        rows = self.session.execute(query)
        return rows

    def query_two(self, product_id, star_rating):
        query = f"SELECT review_id FROM reviews_by_product WHERE product_id='{product_id}' AND star_rating={star_rating};"
        rows = self.session.execute(query)
        return rows

    def query_three(self, customer_id):
        query = f"SELECT review_id FROM reviews_by_customer WHERE customer_id='{customer_id}';"
        rows = self.session.execute(query)
        return rows

    def query_four(self):
        query = f"SELECT COUNT(product_id) AS count, product_id, review_date FROM reviews_by_product GROUP BY product_id;"
        rows = self.session.execute(query)
        return rows

    def query_five(self):
        query = f"SELECT COUNT(customer_id) AS count, customer_id, review_date, verified_purchase FROM reviews_by_customer GROUP BY customer_id;"
        rows = self.session.execute(query)
        return rows

    def query_six_seven(self):
        query = f"SELECT COUNT(customer_id) AS count, customer_id, review_date, star_rating FROM reviews_by_customer GROUP BY customer_id;"
        rows = self.session.execute(query)
        return rows
