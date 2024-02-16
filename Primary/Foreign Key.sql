ALTER TABLE orders_table
ADD CONSTRAINT fk_store_details FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code),
ADD CONSTRAINT fk_products FOREIGN KEY (product_code) REFERENCES dim_products (product_code),
ADD CONSTRAINT fk_date_times FOREIGN KEY (date_uuid) REFERENCES dim_date_times (date_uuid),
ADD CONSTRAINT fk_card_details FOREIGN KEY (card_number) REFERENCES dim_card_details (card_number),
ADD CONSTRAINT fk_users FOREIGN KEY (user_uuid) REFERENCES dim_users (user_uuid);


ALTER TABLE dim_store_details
ADD CONSTRAINT pk_store_details PRIMARY KEY (store_code);

ALTER TABLE dim_products
ADD CONSTRAINT pk_products PRIMARY KEY (product_code);

ALTER TABLE dim_date_times
ADD CONSTRAINT pk_date_times PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
ADD CONSTRAINT pk_card_details PRIMARY KEY (card_number);

ALTER TABLE dim_users
ADD CONSTRAINT pk_users PRIMARY KEY (user_uuid);





