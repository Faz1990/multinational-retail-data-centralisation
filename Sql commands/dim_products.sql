UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(255);


UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
    ELSE 'Unknown' 
END;

SELECT DISTINCT still_available FROM dim_products;

SELECT *
FROM dim_products

ALTER TABLE dim_products
RENAME COLUMN removed to still_available




ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(13),
ALTER COLUMN product_code TYPE VARCHAR(255), 
ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
ALTER COLUMN weight_class TYPE VARCHAR(255);


SELECT column_name
FROM information_schema.columns
WHERE table_name = 'dim_products';


SELECT DISTINCT product_code
FROM orders_table
WHERE product_code NOT IN (SELECT product_code FROM dim_products);



