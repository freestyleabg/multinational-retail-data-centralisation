-- Milestone 3.1
-- Altering 'orders_table' with various column type changes
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN card_number TYPE varchar(19),
    ALTER COLUMN store_code TYPE varchar(12),
    ALTER COLUMN product_code TYPE varchar(11),
    ALTER COLUMN product_quantity TYPE int2;

-- Milestone 3.2
-- Altering 'dim_users_table' with various column type changes
ALTER TABLE dim_users_table
    ALTER COLUMN first_name TYPE varchar(255),
    ALTER COLUMN last_name TYPE varchar(255),
    ALTER COLUMN date_of_birth TYPE date,
    ALTER COLUMN country_code TYPE varchar(2),
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
    ALTER COLUMN join_Date TYPE date;

-- Milestone 3.3
-- Altering 'dim_store_details' with various column type changes
ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE float4 USING longitude::real,
    ALTER COLUMN locality TYPE varchar(255),
    ALTER COLUMN store_code TYPE varchar(12),
    ALTER COLUMN staff_numbers TYPE int2 USING staff_numbers::int2,
    ALTER COLUMN opening_date TYPE date,
    ALTER COLUMN store_type TYPE varchar(255),
    ALTER COLUMN latitude TYPE float4 USING latitude::real,
    ALTER COLUMN country_code TYPE varchar(3),
    ALTER COLUMN continent TYPE varchar(255);

-- Milestone 3.4
-- Updating 'dim_products' by removing currency symbol from 'product_price'
UPDATE
    dim_products
SET
    product_price = REPLACE(product_price, '£', '');

-- Milestone 3.5
-- More alterations to 'dim_products' and renaming a column
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE float4 USING product_price::real,
    ALTER COLUMN weight TYPE float4 USING WEIGHT::real,
    ALTER COLUMN "EAN" TYPE varchar(20),
    ALTER COLUMN product_code TYPE varchar(20),
    ALTER COLUMN date_added TYPE date,
    ALTER COLUMN uuid TYPE uuid USING uuid::uuid,
    ALTER COLUMN weight_class TYPE varchar(20);

ALTER TABLE dim_products
    RENAME COLUMN removed TO still_available;

-- Milestone 3.6
-- Altering 'dim_date_times' with various column type changes
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE varchar(2),
    ALTER COLUMN year TYPE varchar(4),
    ALTER COLUMN day TYPE varchar(2),
    ALTER COLUMN time_period TYPE varchar(20),
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

-- Milestone 3.7
-- Altering 'dim_card_details' with various column type changes
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE varchar(22),
    ALTER COLUMN expiry_date TYPE varchar(5),
    ALTER COLUMN date_payment_confirmed TYPE date USING date_payment_confirmed::date;

-- Milestone 3.8
-- Adding primary keys to various tables
ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);

ALTER TABLE dim_date_times
    ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);

ALTER TABLE dim_users_table
    ADD PRIMARY KEY (user_uuid);

-- Milestone 3.9
-- Adding foreign key constraints to 'orders_table'
ALTER TABLE orders_table
    ADD CONSTRAINT orders_table_card_pkey
    FOREIGN KEY(card_number) 
    REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table
    ADD CONSTRAINT orders_table_date_pkey
    FOREIGN KEY(date_uuid) 
    REFERENCES dim_date_times (date_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT orders_table_product_pkey
    FOREIGN KEY(product_code) 
    REFERENCES dim_products (product_code);

ALTER TABLE orders_table
    ADD CONSTRAINT orders_table_store_pkey
    FOREIGN KEY(store_code) 
    REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table
    ADD CONSTRAINT orders_table_user_pkey
    FOREIGN KEY(user_uuid) 
    REFERENCES dim_users_table (user_uuid);


-- Milestone 4.1
-- Query to select country code and count of distinct store codes
SELECT s.country_code, COUNT(DISTINCT s.store_code)
FROM orders_table o
JOIN dim_store_details s ON s.store_code = o.store_code
GROUP BY s.country_code;

-- Milestone 4.2
-- Query to select locality and count of distinct store codes, ordered and limited
SELECT s.locality, COUNT(DISTINCT s.store_code) as num_stores
FROM orders_table o
JOIN dim_store_details s ON s.store_code = o.store_code
GROUP BY s.locality
ORDER BY num_stores DESC
LIMIT 7;

-- Milestone 4.3
-- Query to select total sales and month, ordered and limited
SELECT ROUND(SUM(o.product_quantity * p.product_price)::NUMERIC, 2)  as total_sales, d.month
FROM orders_table o
JOIN dim_date_times d ON d.date_uuid = o.date_uuid
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY d.month
ORDER BY total_sales DESC
LIMIT 6;

-- Milestone 4.4
-- Query to select total sales, total products sold, and location
SELECT
    COUNT(*) as total_sales,
    SUM(o.product_quantity) as total_products_sold, 
    CASE 
        WHEN s.store_type = 'Web Portal' THEN 'Online'
        ELSE 'Offline' 
    END AS location
FROM orders_table o
JOIN dim_store_details s ON s.store_code = o.store_code
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY location;

-- Milestone 4.5
-- Query to select store type, total sales, and percentage of total
WITH global_products_sold AS (
    SELECT (SUM(o.product_quantity* p.product_price)) AS total
    FROM orders_table o
    JOIN dim_products p  ON o.product_code = p.product_code
)

SELECT
    s.store_type,
    ROUND(SUM(o.product_quantity * p.product_price)::NUMERIC,2) as total_sales,
    ROUND(((SUM(o.product_quantity * p.product_price) * 100) / (SELECT total FROM global_products_sold))::NUMERIC, 2) AS percentage_total
FROM orders_table o
JOIN dim_store_details s ON s.store_code = o.store_code
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY s.store_type
ORDER BY total_sales DESC;

-- Milestone 4.6
-- Query to select total sales, year, and month, ordered and limited
SELECT
    ROUND(SUM(o.product_quantity * p.product_price)::NUMERIC,2) as total_sales,
    d.year,
    d.month
FROM orders_table o
JOIN dim_store_details s ON s.store_code = o.store_code
JOIN dim_products p ON o.product_code = p.product_code
JOIN dim_date_times d on d.date_uuid = o.date_uuid
GROUP BY d.year, d.month
ORDER BY total_sales DESC
LIMIT 10;

-- Milestone 4.7
-- Query to select total staff numbers and country code, ordered
SELECT
    SUM(staff_numbers)::NUMERIC as total_staff_numbers,
    country_code
FROM dim_store_details s 
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- Milestone 4.8
-- Query to select total sales, store type, and country code for 'DE', ordered and limited
SELECT
    ROUND(SUM(o.product_quantity * p.product_price)::NUMERIC,2) as total_sales,
    s.store_type,
    s.country_code
    
FROM orders_table o
JOIN dim_store_details s ON s.store_code = o.store_code
JOIN dim_products p ON o.product_code = p.product_code
WHERE s.country_code = 'DE'
GROUP BY s.country_code, s.store_type
ORDER BY total_sales ASC
LIMIT 10;

-- Milestone 4.9
-- Query with CTE to select year and average actual time taken, ordered and limited
WITH cte AS (
    SELECT
        year,
        datetime,
        LEAD (datetime, 1)
        OVER (
            PARTITION BY year
            ORDER BY datetime
        ) as next_datetime
    FROM dim_date_times
    )

SELECT
    year,
    AVG(next_datetime - datetime) as actual_time_taken
FROM cte
GROUP BY year
ORDER BY actual_time_taken DESC
LIMIT 5;