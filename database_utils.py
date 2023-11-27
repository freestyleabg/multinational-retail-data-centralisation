import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd

class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as f:
            data_loaded = yaml.safe_load(f)
        return data_loaded
    
    def init_db_engine(self, creds):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds['RDS_HOST']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        DATABASE = creds['RDS_DATABASE']
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return engine

    def list_db_tables(self, engine):
        with engine.connect() as conn:
            inspector = inspect(conn)
            return inspector.get_table_names()


connector = DatabaseConnector()
creds = connector.read_db_creds()
engine = connector.init_db_engine(creds)
tables_list = connector.list_db_tables(engine)
print(tables_list)