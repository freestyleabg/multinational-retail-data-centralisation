import yaml
from sqlalchemy import create_engine, inspect
# import pandas as pd


class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self, yaml_file):
        with open(yaml_file, "r") as f:
            data_loaded = yaml.safe_load(f)
        return data_loaded

    def init_db_engine(self, creds):
        DATABASE_TYPE = "postgresql"
        DBAPI = "psycopg2"
        HOST = creds["RDS_HOST"]
        USER = creds["RDS_USER"]
        PASSWORD = creds["RDS_PASSWORD"]
        DATABASE = creds["RDS_DATABASE"]
        PORT = 5432
        self.engine = create_engine(
            f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        )
        return self.engine

    def list_db_tables(self):
        with self.engine.connect() as conn:
            inspector = inspect(conn)
            return inspector.get_table_names()

    def upload_to_db(self, df, table_name):
        with self.engine.connect() as conn:
            try:
                df.to_sql(table_name, conn, index=False)
            except ValueError as err:
                print(err.__str__())
            else:
                print(f"{table_name} T")


# connector = DatabaseConnector()
# creds = connector.read_db_creds('db_creds.yaml')
# engine = connector.init_db_engine(creds)
# list_of_tables = connector.list_db_tables()
# print(list_of_tables)
