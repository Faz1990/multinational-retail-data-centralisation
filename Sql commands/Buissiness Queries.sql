WITH ordered_sales AS (
  SELECT
    EXTRACT(YEAR FROM datetime) AS yr,
    datetime,
    LEAD(datetime) OVER (PARTITION BY EXTRACT(YEAR FROM datetime) ORDER BY datetime) - datetime AS time_diff
  FROM
    dim_date_times
  JOIN
    orders_table ON dim_date_times.date_uuid = orders_table.date_uuid
)
, avg_time_diff AS (
  SELECT
    yr,
    AVG(time_diff) AS avg_time_diff
  FROM
    ordered_sales
  GROUP BY
    yr
)
SELECT
  yr,
  json_build_object(
    'hours', EXTRACT(HOUR FROM avg_time_diff),
    'minutes', EXTRACT(MINUTE FROM avg_time_diff),
    'seconds', EXTRACT(SECOND FROM avg_time_diff),
    'milliseconds', (EXTRACT(MILLISECOND FROM avg_time_diff)::text)
  ) AS actual_time_taken
FROM
  avg_time_diff
ORDER BY
  yr DESC;



WITH store_info_de AS (
  SELECT
    store_code,
    store_type
  FROM
    dim_store_details  
  WHERE
    country_code = 'DE'
),
store_revenues AS (
  SELECT
    o.store_code,
    SUM(o.product_quantity * dp.product_price) AS store_revenue
  FROM
    orders_table o
  JOIN
    dim_products dp ON o.product_code = dp.product_code
  GROUP BY
    o.store_code
)
SELECT
  i.store_type,
  SUM(r.store_revenue) AS total_sales,  
  'DE' AS country_code
FROM
  store_info_de i
JOIN
  store_revenues r ON i.store_code = r.store_code
GROUP BY
  i.store_type
ORDER BY
  total_sales DESC;


SELECT
  SUM(staff_numbers) AS total_staff_numbers,
  country_code
FROM
  dim_store_details  
GROUP BY
  country_code
ORDER BY
  total_staff_numbers DESC;



WITH times AS (
  SELECT
    date_uuid,
    datetime,
    TO_CHAR(datetime, 'MM') AS month,
    TO_CHAR(datetime, 'YYYY') AS year
  FROM dim_date_times
),
revenues AS (
  SELECT
    o.date_uuid,
    o.product_quantity * dp."product_price" AS prod_rev
  FROM orders_table o
  JOIN dim_products dp ON o.product_code = dp.product_code
)
SELECT
  SUM(revenues.prod_rev) AS total_sales,
  times.year,
  times.month
FROM times
JOIN revenues ON times.date_uuid = revenues.date_uuid
GROUP BY times.year, times.month
ORDER BY total_sales DESC;




WITH revenue_calculated AS (
  SELECT
    o.store_code,
    o.product_quantity * dp."product_price" AS revenue
  FROM
    orders_table o
  JOIN
    dim_products dp ON o.product_code = dp.product_code
),
store_types AS (
  SELECT DISTINCT
    ds.store_type,
    o.store_code
  FROM
    dim_store_details ds
  JOIN
    orders_table o ON ds.store_code = o.store_code
),
total_sales AS (
  SELECT
    SUM(revenue) AS overall_total_sales
  FROM
    revenue_calculated rc
  JOIN
    store_types st ON rc.store_code = st.store_code
  WHERE
    store_type IS NOT NULL
)
SELECT
  st.store_type,
  SUM(rc.revenue) AS total_sales,
  (SUM(rc.revenue) / (SELECT overall_total_sales FROM total_sales)) * 100 AS percentage_total_sales
FROM
  revenue_calculated rc
JOIN
  store_types st ON rc.store_code = st.store_code
WHERE
  store_type IS NOT NULL
GROUP BY
  st.store_type
ORDER BY
  total_sales DESC;



SELECT
  COUNT(*) AS number_of_sales,
  SUM(o.product_quantity) AS product_quantity_count,
  'online' AS location
FROM
  orders_table o
JOIN
  dim_products dp ON o.product_code = dp.product_code
WHERE
  store_code LIKE 'WEB%'
UNION
SELECT
  COUNT(*) AS number_of_sales,
  SUM(o.product_quantity) AS product_quantity_count,
  'offline' AS location
FROM
  orders_table o
JOIN
  dim_products dp ON o.product_code = dp.product_code
WHERE
  store_code NOT LIKE 'WEB%';





WITH a AS (
  SELECT
    o.date_uuid,
    (CAST(dp.product_price AS DOUBLE PRECISION) * o.product_quantity) AS product_revenue
  FROM orders_table o
  JOIN dim_products dp ON o.product_code = dp.product_code
),
b AS (
  SELECT
    month,
    date_uuid
  FROM dim_date_times
)
SELECT
  SUM(a.product_revenue) AS total_sales,
  b.month
FROM a
JOIN b ON a.date_uuid = b.date_uuid
GROUP BY b.month
ORDER BY total_sales DESC;




SELECT locality, count(*) as total_number_of_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_number_of_stores DESC


SELECT country_code, count(*) as total_number_of_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_number_of_stores DESC