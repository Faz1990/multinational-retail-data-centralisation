ALTER TABLE dim_users
  ALTER COLUMN first_name TYPE VARCHAR(255),
  ALTER COLUMN last_name TYPE VARCHAR(255),
  ALTER COLUMN date_of_birth TYPE DATE USING (date_of_birth::DATE),
  ALTER COLUMN join_date TYPE DATE USING (join_date::DATE),
  ALTER COLUMN user_uuid TYPE UUID USING (user_uuid::UUID),
  ALTER COLUMN country_code TYPE VARCHAR(2);

SELECT MAX(LENGTH(country_code)) FROM dim_users

SELECT DISTINCT user_uuid
FROM orders_table
WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);

ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING(user_uuid::UUID)