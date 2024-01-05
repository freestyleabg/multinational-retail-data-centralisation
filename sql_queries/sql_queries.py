from sqlalchemy import text

from database_utils import DatabaseConnector

# %% Milestone 3.1
local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    conn.execute(
        text(
            """
                ALTER TABLE orders_table
                    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
                    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
                    ALTER COLUMN card_number TYPE varchar(19),
                    ALTER COLUMN store_code TYPE varchar(12),
                    ALTER COLUMN product_code TYPE varchar(11),
                    ALTER COLUMN product_quantity TYPE int2
            """
        )
    )
    conn.commit()
# %% Milestone 3.2
local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    conn.execute(
        text(
            """
                ALTER TABLE dim_users_table
                    ALTER COLUMN first_name TYPE varchar(255),
                    ALTER COLUMN last_name TYPE varchar(255),
                    ALTER COLUMN date_of_birth TYPE date,
                    ALTER COLUMN country_code TYPE varchar(2),
                    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
                    ALTER COLUMN join_Date TYPE date
            """
        )
    )
    conn.commit()
# %% Milestone 3.3
local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    conn.execute(
        text(
            """
                ALTER TABLE dim_store_details
                    ALTER COLUMN longitude TYPE float4 USING longitude::real,
                    ALTER COLUMN locality TYPE varchar(255),
                    ALTER COLUMN store_code TYPE varchar(12),
                    ALTER COLUMN staff_numbers TYPE int2 USING staff_numbers::int2,
                    ALTER COLUMN opening_date TYPE date,
                    ALTER COLUMN store_type TYPE varchar(255),
                    ALTER COLUMN latitude TYPE float4 USING latitude::real,
                    ALTER COLUMN country_code TYPE varchar(3),
                    ALTER COLUMN continent TYPE varchar(255)


            """
        )
    )
    conn.commit()
# %% Milestone 3.4
local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    conn.execute(
        text(
            """
                UPDATE
                    dim_products
                SET
                    product_price = REPLACE(product_price, '£', '')
            """
        )
    )
    conn.commit()
# %%
local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    conn.execute(
        text(
            """
                ALTER TABLE dim_products
                    ADD COLUMN IF NOT EXISTS weight_class varchar(15);

                UPDATE
                    dim_products
                SET
                    weight_class = CASE
                                     WHEN weight < 2 THEN 'Light'
                                     WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
                                     WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
                                     WHEN weight >= 140 THEN 'Truck_Required'
                                    END;
            """
        )
    )
    conn.commit()
# %% Milestone 3.5
local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    conn.execute(
        text(
            """
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
            """
        )
    )
    conn.commit()


# %% Milestone 3.6
local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    conn.execute(
        text(
            """
                ALTER TABLE dim_date_times
                    ALTER COLUMN month TYPE varchar(2),
                    ALTER COLUMN year TYPE varchar(4),
                    ALTER COLUMN day TYPE varchar(2),
                    ALTER COLUMN time_period TYPE varchar(20),
                    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid
            """
        )
    )
    conn.commit()
# %% Milestone 3.7
local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    conn.execute(
        text(
            """
                ALTER TABLE dim_card_details
                    ALTER COLUMN card_number TYPE varchar(22),
                    ALTER COLUMN expiry_date TYPE varchar(5),
                    ALTER COLUMN date_payment_confirmed TYPE date USING date_payment_confirmed::date
            """
        )
    )
    conn.commit()
# %% Milestone 3.8
local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    conn.execute(
        text(
            """
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
            """
        )
    )
    conn.commit()
# %% Milestone 3.9

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    conn.execute(
        text(
            """
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
                
            """
        )
    )
    conn.commit()
# %% Milestone 4.1

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    result = conn.execute(
        text(
            """
                SELECT s.country_code, COUNT(DISTINCT s.store_code)
                FROM orders_table o
                JOIN dim_store_details s ON s.store_code = o.store_code
                GROUP BY s.country_code
            """
        )
    )
    for row in result:
        print(row)

# %% Milestone 4.2

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    result = conn.execute(
        text(
            """
                SELECT s.locality, COUNT(DISTINCT s.store_code) as num_stores
                FROM orders_table o
                JOIN dim_store_details s ON s.store_code = o.store_code
                GROUP BY s.locality
                ORDER BY num_stores DESC
                LIMIT 7
            """
        )
    )
    for row in result:
        print(row)

# %% Milestone 4.3

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    result = conn.execute(
        text(
            """
                SELECT ROUND(SUM(o.product_quantity * p.product_price)::NUMERIC, 2)  as total_sales, d.month
                FROM orders_table o
                JOIN dim_date_times d ON d.date_uuid = o.date_uuid
                JOIN dim_products p ON o.product_code = p.product_code
                GROUP BY d.month
                ORDER BY total_sales DESC
                LIMIT 6
            """
        )
    )
    for row in result:
        print(row)

# %% Milestone 4.4

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    result = conn.execute(
        text(
            """
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
                GROUP BY location
            """
        )
    )
    for row in result:
        print(row)

# %% Milestone 4.5

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    result = conn.execute(
        text(
            """
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
            """
        )
    )
    for row in result:
        print(row)
# %% Milestone 4.6

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    result = conn.execute(
        text(
            """
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
            """
        )
    )
    for row in result:
        print(row)

# %% Milestone 4.7

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    result = conn.execute(
        text(
            """
                SELECT
                    SUM(staff_numbers)::NUMERIC as total_staff_numbers,
                    country_code
                FROM dim_store_details s 
                GROUP BY country_code
                ORDER BY total_staff_numbers DESC
            """
        )
    )
    for row in result:
        print(row)

# %% Milestone 4.8

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    result = conn.execute(
        text(
            """
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
            """
        )
    )
    for row in result:
        print(row)


# %% Milestone 4.9

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
with engine.connect() as conn:
    result = conn.execute(
        text(
            """
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
            """
        )
    )
    for row in result:
        print(row)
