import database_utils as du
import pandas as pd

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
            return print(rds_table)

trial = du.DatabaseConnector()
extractor = DataExtractor()
extractor.read_rds_table(trial, 'legacy_users')

# connector = DatabaseConnector()
# creds = connector.read_db_creds()
# engine = connector.init_db_engine(creds)
# tables_list = connector.list_db_tables(engine)
# print(tables_list