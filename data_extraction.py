# %% Run this code cell below
import re
from io import BytesIO, StringIO

import boto3
import pandas as pd
import requests
import tabula
from IPython.display import display

from data_cleaning import DataCleaning


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
    def retrieve_stores_data(url, headers, number_of_stores):
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
