
CREATE TABLE product_counter (product_id int, counter int, review text, customer_id int, PRIMARY KEY ((product_id), counter) );

CREATE TABLE product_rating (product_id int, star_rating int, review text, PRIMARY KEY((product_id, star_rating)) );

CREATE TABLE customer_rating (customer_id int, star_rating int, review text, PRIMARY KEY ((customer_id), star_rating) );