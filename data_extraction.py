# %%
import database_utils as du
import pandas as pd

# %%
class DataExtractor:
    def __init__(self):
        # self.connector = du.DatabaseConnector()
        # self.creds = self.connector.read_db_creds()
        # self.engine = self.connector.init_db_engine()
        pass

    def read_rds_table(self, instance, table): 
        creds = instance.read_db_creds()
        engine = instance.init_db_engine(creds)
        with engine.connect() as conn:
            rds_table = pd.read_sql_table(table, conn)
            return rds_table

trial = du.DatabaseConnector()
extractor = DataExtractor()
df = extractor.read_rds_table(trial, 'legacy_users')
print(df)
# %%