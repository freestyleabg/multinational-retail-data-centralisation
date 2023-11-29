# %%
import database_utils as du
import pandas as pd
import data_cleaning as dc
# %%
class DataExtractor:
    def __init__(self):
        # self.connector = du.DatabaseConnector()
        # self.creds = self.connector.read_db_creds()
        # self.engine = self.connector.init_db_engine()
        pass

    def read_rds_table(self, instance, table, creds_yaml): 
        creds = instance.read_db_creds(creds_yaml)
        engine = instance.init_db_engine(creds)
        with engine.connect() as conn:
            rds_table = pd.read_sql_table(table, conn)
            return rds_table

connector = du.DatabaseConnector()
extractor = DataExtractor()
df = extractor.read_rds_table(connector, 'legacy_users', 'db_creds.yaml')
print(df)
cleaner = dc.DataCleaning()
cleaner.clean_user_data(df, index_col='index')


connector1 = du.DatabaseConnector()
creds1 = connector1.read_db_creds('db_creds_local.yaml')
engine = connector1.init_db_engine(creds1)
connector1.upload_to_db(df, 'sales_data')
# %%