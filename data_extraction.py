from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
import tabula
import requests
import boto3

# Let's use Amazon S3
s3 = boto3.resource('s3')
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


# Milestone 2.3
aws_connector = DatabaseConnector()
extractor = DataExtractor()
user_df = extractor.read_rds_table(aws_connector, "legacy_users", "db_creds.yaml")
cleaner = DataCleaning()
cleaner.clean_user_data(user_df, index_col="index")

local_connector = DatabaseConnector()
creds1 = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(creds1)
local_connector.upload_to_db(user_df, "dim_users")

# Milestone 2.4
card_df = DataExtractor.retrieve_pdf_data(
    "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
)
cleaner.clean_card_data(card_df)
local_connector.upload_to_db(card_df, "dim_card_details")

# Milestone 2.5
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
cleaner.clean_store_data(store_df, index_col='index')
local_connector.upload_to_db(store_df, "dim_store_details")

# Milestone 2.6