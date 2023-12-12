# %% Run this code cell
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
import tabula
import requests
import boto3
from io import BytesIO, StringIO
from IPython.display import display
import re
from sqlalchemy import text


def list_buckets():
    # Let's use Amazon S3
    s3 = boto3.resource("s3")
    # Print out bucket names
    for bucket in s3.buckets.all():
        print(bucket.name)


class DataExtractor:
    def __init__(self):
        # self.connector = du.DatabaseConnector()
        # self.creds = self.connector.read_db_creds()
        # self.engine = self.connector.init_db_engine()
        pass

    @staticmethod
    def read_rds_table(instance, table, creds_yaml):
        creds = instance.read_db_creds(creds_yaml)
        engine = instance.init_db_engine(creds)
        with engine.connect() as conn:
            rds_table = pd.read_sql_table(table, conn)
            return rds_table

    @staticmethod
    def retrieve_pdf_data(url):
        df = tabula.read_pdf(url, "dataframe", pages="all", multiple_tables=False)
        return df[0]

    @staticmethod
    def list_number_of_stores(url, headers):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            number_of_stores = data["number_stores"]
            print(f"Number of stores: {data['number_stores']}")
        return number_of_stores

    @staticmethod
    def retrieve_stores_data(url, headers):
        store_json_list = []
        for store_num in range(number_of_stores):
            response = requests.get(url.format(store_number=store_num), headers=headers)
            if response.status_code == 200:
                data = response.json()
            store_json = data
            store_json_list.append(store_json)
        store_df = pd.json_normalize(store_json_list)
        return store_df

    @staticmethod
    def extract_from_s3(url):
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
    def print_df(df, head=100):
        with pd.option_context("display.max_rows", None, "display.max_columns", None):
            display(df.head(head))


# %%
if __name__ == "__main__":
    # %% Milestone 2.3
    extractor = DataExtractor()
    cleaner = DataCleaning()

    aws_connector = DatabaseConnector()
    user_df = extractor.read_rds_table(aws_connector, "legacy_users", "db_creds.yaml")
    cleaner.clean_user_data(user_df, index_col="index")

    local_connector = DatabaseConnector()
    local_creds = local_connector.read_db_creds("db_creds_local.yaml")
    engine = local_connector.init_db_engine(local_creds)
    local_connector.upload_to_db(user_df, "dim_users")

    # %% Milestone 2.4
    extractor = DataExtractor()
    cleaner = DataCleaning()

    card_df = DataExtractor.retrieve_pdf_data(
        "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    )
    cleaner.clean_card_data(card_df)
    local_connector.upload_to_db(card_df, "dim_card_details")

    # %% Milestone 2.5
    extractor = DataExtractor()
    cleaner = DataCleaning()
    
    local_connector = DatabaseConnector()
    local_creds = local_connector.read_db_creds("db_creds_local.yaml")
    engine = local_connector.init_db_engine(local_creds)

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
    date_df = date_df.reindex(columns=["datetime", "time_period", "date_uuid"])

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
                        ALTER COLUMN country_code TYPE varchar(2),
                        ALTER COLUMN continent TYPE varchar(255)


                """
            )
        )
        conn.commit()
# %%
