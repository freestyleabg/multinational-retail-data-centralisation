import yaml
from sqlalchemy import create_engine, inspect


class DatabaseConnector:
    """
    This class provides methods to connect to a database, read credentials, list database tables, and upload data.
    """

    def __init__(self):
        """
        Initializes an instance of the DatabaseConnector class.
        """
        pass

    def read_db_creds(self, yaml_file):
        """
        Reads database credentials from a YAML file.

        Args:
            yaml_file (str): The path to the YAML file containing database credentials.

        Returns:
            dict: A dictionary containing database credentials.
        """
        with open(yaml_file, "r") as f:
            data_loaded = yaml.safe_load(f)
        return data_loaded

    def init_db_engine(self, creds):
        """
        Initializes a database engine using credentials.

        Args:
            creds (dict): A dictionary containing database credentials.

        Returns:
            sqlalchemy.engine.base.Engine: A SQLAlchemy engine instance.
        """
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
        """
        Lists all tables in the connected database.

        Returns:
            list: A list of table names in the database.
        """
        with self.engine.connect() as conn:
            inspector = inspect(conn)
            return inspector.get_table_names()

    def upload_to_db(self, df, table_name):
        """
        Uploads a DataFrame to a database table.

        Args:
            df (pandas.DataFrame): The DataFrame to upload.
            table_name (str): The name of the database table to which the DataFrame will be uploaded.

        The method prints a success message or an error if the upload fails.
        """
        with self.engine.connect() as conn:
            try:
                df.to_sql(table_name, conn, index=False)
            except ValueError as err:
                print(err.__str__())
            else:
                print(f"{table_name} connected.")
