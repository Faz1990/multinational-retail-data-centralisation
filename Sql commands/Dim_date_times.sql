ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(250), 
ALTER COLUMN year TYPE VARCHAR(10),
ALTER COLUMN day TYPE VARCHAR(10),
ALTER COLUMN time_period TYPE VARCHAR(10), 
ALTER COLUMN date_uuid TYPE UUID USING (date_uuid::uuid);

SELECT MAX(YEAR)
FROM dim_date_times

SELECT*
FROM dim_date_times

SELECT DISTINCT(date_uuid)
FROM orders_table
WHERE date_uuid NOT IN (SELECT date_uuid FROM dim_date_times)