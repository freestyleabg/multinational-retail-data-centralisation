# %% Run this code cell below
from data_cleaning import DataCleaning
from data_extraction import DataExtractor
from database_utils import DatabaseConnector

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

api_key = open("config/api_key", "r").read()
store_api_data = {
    "endpoints": {
        "number_stores": "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores",
        "store_details": "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}",
    },
    "headers": {"x-api-key": api_key},
}

number_of_stores = extractor.list_number_of_stores(
    store_api_data["endpoints"]["number_stores"], store_api_data["headers"]
)
store_df = extractor.retrieve_stores_data(
    store_api_data["endpoints"]["store_details"],
    store_api_data["headers"],
    number_of_stores,
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

local_connector = DatabaseConnector()
local_creds = local_connector.read_db_creds("db_creds_local.yaml")
engine = local_connector.init_db_engine(local_creds)
local_connector.upload_to_db(date_df, "dim_date_times")