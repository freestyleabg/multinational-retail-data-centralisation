# %% Run this code cell below
import re
from io import BytesIO, StringIO

import boto3
import pandas as pd
import requests
import tabula
from IPython.display import display
from sqlalchemy import text

from data_cleaning import DataCleaning
from database_utils import DatabaseConnector


def list_buckets():
    """
    Lists all the buckets in the connected AWS S3 resource.
    """
    s3 = boto3.resource("s3")
    for bucket in s3.buckets.all():
        print(bucket.name)


class DataExtractor:
    """
    This class provides methods for extracting data from various sources including RDS, PDFs, APIs, and S3.
    """

    def __init__(self):
        """
        Initializes an instance of the DataExtractor class.
        """
        pass

    @staticmethod
    def read_rds_table(instance, table, creds_yaml):
        """
        Reads a table from a relational database (RDS) using the provided instance of the DatabaseConnector class,
        the table name, and the path to the YAML file containing the database credentials.

        Args:
            instance (DatabaseConnector): An instance of the DatabaseConnector class used to connect to the database.
            table (str): The name of the table to read from the database.
            creds_yaml (str): The path to the YAML file containing the database credentials.

        Returns:
            pandas.DataFrame: The table data as a pandas DataFrame.
        """
        creds = instance.read_db_creds(creds_yaml)
        engine = instance.init_db_engine(creds)
        with engine.connect() as conn:
            rds_table = pd.read_sql_table(table, conn)
            return rds_table

    @staticmethod
    def retrieve_pdf_data(url):
        """
        Retrieves data from a PDF file located at the given URL.

        Args:
            url (str): The URL of the PDF file.

        Returns:
            pandas.DataFrame: The data extracted from the PDF as a DataFrame.
        """
        df = tabula.read_pdf(url, "dataframe", pages="all", multiple_tables=False)
        return df[0]

    @staticmethod
    def list_number_of_stores(url, headers):
        """
        Retrieves the number of stores from an API endpoint.

        Args:
            url (str): The URL of the API endpoint to get the number of stores.
            headers (dict): The headers to be used in the API request.

        Returns:
            int: The number of stores.
        """
        response = requests.get(url, headers=headers, timeout=60)
        if response.status_code == 200:
            data = response.json()
            number_of_stores = data["number_stores"]
            print(f"Number of stores: {data['number_stores']}")
        return number_of_stores

    @staticmethod
    def retrieve_stores_data(url, headers):
        """
        Retrieves data for each store from an API endpoint and compiles it into a DataFrame.

        Args:
            url (str): The URL of the API endpoint to get store details. The URL should have a placeholder for the store number.
            headers (dict): The headers to be used in the API request.

        Returns:
            pandas.DataFrame: The compiled store data as a DataFrame.
        """
        store_json_list = []
        for store_num in range(number_of_stores):
            response = requests.get(
                url.format(store_number=store_num), headers=headers, timeout=60
            )
            if response.status_code == 200:
                data = response.json()
            store_json = data
            store_json_list.append(store_json)
        store_df = pd.json_normalize(store_json_list)
        return store_df

    @staticmethod
    def extract_from_s3(url):
        """
        Extracts data from a file stored in an S3 bucket.

        Args:
            url (str): The S3 URL of the file to be extracted.

        Returns:
            pandas.DataFrame: The data extracted from the file as a DataFrame.
        """
        match = re.match(
            r"(s3|http|https)://(?P<bucket>[-a-zA-Z]+)\.?[a-zA-Z0-9.-]*/(?P<path>.+)",
            url,
        )
        bucket_info = match.groupdict()
        bucket_name = bucket_info["bucket"]
        file_path = bucket_info["path"]

        file_buffer = BytesIO()

        s3 = boto3.client("s3")

        response = s3.get_object(Bucket=bucket_name, Key=file_path)
        if "csv" in response["ContentType"]:
            csv_stream = response["Body"].read().decode("utf-8")
            csv_file = StringIO(csv_stream)
            df = pd.read_csv(csv_file)
            return df
        if "json" in response["ContentType"]:
            s3.download_fileobj(Bucket=bucket_name, Key=file_path, Fileobj=file_buffer)
            byte_value = file_buffer.getvalue()
            str_value = byte_value.decode("utf-8")
            json_file = StringIO(str_value)
            df = pd.read_json(json_file)
            return df

    @staticmethod
    def print_df(df, head=100000):
        """
        Prints the specified number of rows of a DataFrame.

        Args:
            df (pandas.DataFrame): The DataFrame to be printed.
            head (int, optional): The number of rows to print. Defaults to 100000.
        """
        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            display(df.head(head))


# %% Milestone 2.3
extractor = DataExtractor()
cleaner = DataCleaning()

aws_connector = DatabaseConnector()
user_df = extractor.read_rds_table(aws_connector, "legacy_users", "db_creds.yaml")
cleaner.clean_user_data(user_df, index_col="index")

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
local_connector.upload_to_db(user_df, "dim_users_table")

# %% Milestone 2.4
extractor = DataExtractor()
cleaner = DataCleaning()

card_df = DataExtractor.retrieve_pdf_data(
    "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
)
cleaner.clean_card_data(card_df)

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
local_connector.upload_to_db(card_df, "dim_card_details")

# %% Milestone 2.5
extractor = DataExtractor()
cleaner = DataCleaning()


store_api_data = {
    "endpoints": {
        "number_stores": "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores",
        "store_details": "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}",
    },
    "headers": {"x-api-key": "redacted"},
}

number_of_stores = extractor.list_number_of_stores(
    store_api_data["endpoints"]["number_stores"], store_api_data["headers"]
)
store_df = extractor.retrieve_stores_data(
    store_api_data["endpoints"]["store_details"], store_api_data["headers"]
)
store_df = store_df.reindex(
    columns=[
        "index",
        "store_code",
        "store_type",
        "staff_numbers",
        "address",
        "longitude",
        "latitude",
        "locality",
        "country_code",
        "continent",
        "opening_date",
    ]
)
cleaner.clean_store_data(store_df, index_col="index")

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
local_connector.upload_to_db(store_df, "dim_store_details")

# %% Milestone 2.6
extractor = DataExtractor()
cleaner = DataCleaning()

product_df = DataExtractor.extract_from_s3("s3://data-handling-public/products.csv")

cleaner.clean_unknown_string(product_df)
cleaner.convert_product_weights(product_df)
cleaner.clean_products_data(product_df)
product_df = product_df.reindex(
    columns=[
        "product_name",
        "product_price",
        "weight",
        "category",
        "EAN",
        "date_added",
        "uuid",
        "removed",
        "product_code",
    ]
)
extractor.print_df(product_df, 2000)
local_connector.upload_to_db(product_df, "dim_products")

# %% Milestone 2.7
extractor = DataExtractor()
cleaner = DataCleaning()

aws_connector = DatabaseConnector()
orders_df = extractor.read_rds_table(aws_connector, "orders_table", "db_creds.yaml")
cleaner.clean_orders_data(orders_df)

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
local_connector.upload_to_db(orders_df, "orders_table")

# %% Milestone 2.8
extractor = DataExtractor()
cleaner = DataCleaning()

date_df = extractor.extract_from_s3(
    "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
)
cleaner.clean_date_data(date_df)
# date_df = date_df.reindex(columns=["datetime", "time_period", "date_uuid"])

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
local_connector.upload_to_db(date_df, "dim_date_times")

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