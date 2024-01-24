ALTER TABLE orders_table
ADD CONSTRAINT fk_store_details FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code),
ADD CONSTRAINT fk_products FOREIGN KEY (product_code) REFERENCES dim_products (product_code),
ADD CONSTRAINT fk_date_times FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid),
ADD CONSTRAINT fk_card_details FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number),
ADD CONSTRAINT fk_users FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);


ALTER TABLE orders_table
ADD CONSTRAINT fk_card_details
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number)

ADD CONSTRAINT fk_date_times
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid)

SELECT DISTINCT store_code
FROM orders_table
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

ALTER TABLE orders_table
ADD CONSTRAINT fk_products FOREIGN KEY (product_code) REFERENCES dim_products (product_code)

SELECT*
FROM orders_table

SELECT DISTINCT store_code
FROM orders_table
WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);