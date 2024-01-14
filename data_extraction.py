import re
from io import BytesIO, StringIO

import boto3
import pandas as pd
import requests
import tabula
from IPython.display import display


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
