from cassandra_client import CassandraClient
import pandas as pd


def write(client):
    tsv_file = "./sample.tsv"
    tsv = pd.read_csv(tsv_file, sep='\t')
    tsv = tsv[['customer_id', 'review_id', 'product_id',
               'star_rating', 'review_date', 'verified_purchase']]

    for ind, row in tsv.iterrows():
        client.insert_record(
            table1, row[0], row[1], row[2], row[3], row[4], row[5])
        client.insert_record(
            table2, row[0], row[1], row[2], row[3], row[4], row[5])


if __name__ == "__main__":
    host = 'cassandra-node1'
    port = 9042
    keyspace = 'hw4'

    table1 = "reviews_by_product"
    table2 = "reviews_by_customer"
    table3 = "products_by_review"
    table4 = "customers_by_review"
    table5 = "reviews_by_star"

    client = CassandraClient(host, port, keyspace)
    client.connect()
    write(client)
    client.close()
