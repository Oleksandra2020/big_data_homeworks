docker exec -it cassandra-node1 cqlsh -e "CREATE KEYSPACE hw4 WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 }; USE hw4; CREATE TABLE reviews_by_product (product_id text, customer_id text, review_id text, star_rating int, review_date date, verified_purchase text, PRIMARY KEY((product_id), star_rating)); CREATE TABLE reviews_by_customer (product_id text, customer_id text, review_id text, star_rating int, review_date date, verified_purchase text, PRIMARY KEY(customer_id));"

# docker run -it --network my-cassandra-network --rm cassandra cqlsh cassandra-node1