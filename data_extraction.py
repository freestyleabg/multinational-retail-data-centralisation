from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import pandas as pd
import data_cleaning as dc
import tabula 


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
        df = tabula.read_pdf(url, 'dataframe', pages='all', multiple_tables=False)
        return df[0]

# Milestone 2.3
aws_connector = DatabaseConnector()
extractor = DataExtractor()
user_df = extractor.read_rds_table(aws_connector, 'legacy_users', 'db_creds.yaml')
cleaner = DataCleaning()
cleaner.clean_user_data(user_df, index_col='index')

local_connector = DatabaseConnector()
creds1 = local_connector.read_db_creds('db_creds_local.yaml')
engine = local_connector.init_db_engine(creds1)
local_connector.upload_to_db(user_df, 'dim_users')

card_df = DataExtractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
cleaner.clean_card_data(card_df)
local_connector.upload_to_db(card_df, 'dim_card_details')

#Dont edit till you add comments!
#WUKR5JLBGCF