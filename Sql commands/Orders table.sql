ALTER TABLE orders_table
  ALTER COLUMN date_uuid TYPE UUID USING (date_uuid::UUID),
  ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::UUID),
  ALTER COLUMN card_number TYPE VARCHAR(19),
  ALTER COLUMN store_code TYPE VARCHAR(12),
  ALTER COLUMN product_code TYPE VARCHAR(11),
  ALTER COLUMN product_quantity TYPE SMALLINT USING (product_quantity::SMALLINT);

SELECT MAX(LENGTH(card_number :: TEXT)) AS max_length_card_number FROM orders_table;

SELECT MAX(LENGTH(store_code)) AS max_length_store_code FROM orders_table;

SELECT MAX(LENGTH(product_code)) AS max_length_product_code FROM orders_table;


SELECT* FROM orders_table