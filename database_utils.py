import yaml
from sqlalchemy import create_engine, MetaData
import pandas as pd
import logging
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO)

class DatabaseConnector:
    """
    Class to manage database connections and operations.
    """

    def __init__(self):
        """ Initializes database connectors for RDS and local databases. """
        self.engine = self.init_db_engine()
        self.local_engine = self.init_local_db_engine()

    def read_db_creds(self, filepath='db_creds.yaml'):
        """
        Reads database credentials from a YAML file.
        :param filepath: Path to the YAML file.
        :return: Dictionary of database credentials.
        """
        with open(filepath, 'r') as file:
            return yaml.safe_load(file)

    def init_db_engine(self):
        """
        Initializes the engine for the RDS database.
        :return: SQLAlchemy engine object for the RDS database.
        """
        creds = self.read_db_creds()
        engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    def init_local_db_engine(self):
        """
        Initializes the engine for a local PostgreSQL database.
        :return: SQLAlchemy engine object for the local database.
        """
        username = "postgres"
        password = "Tekken56"
        host = "localhost"
        port = "5432"  # default PostgreSQL port
        db_name = "sales_data"
        local_engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{db_name}")
        return local_engine

    def list_db_tables(self, engine):
        """
        Lists tables in the specified database.
        :param engine: SQLAlchemy engine object connected to the database.
        :return: List of table names in the database.
        """
        meta = MetaData()
        meta.reflect(bind=engine)  
        return [table.name for table in meta.tables.values()]

    def upload_to_db(self, df, table_name, engine):
        """
        Uploads a DataFrame to a specified table in the database.
        :param df: DataFrame to upload.
        :param table_name: Name of the target table in the database.
        :param engine: SQLAlchemy engine object connected to the database.
        """
        try:
            with engine.connect() as connection:  
                with connection.begin() as transaction:  
                    df.to_sql(table_name, connection, if_exists='replace')
                    transaction.commit()  
        except SQLAlchemyError as e:  
            logging.error(f"An SQLAlchemy error occurred: {str(e)}")
        except Exception as e:  
            logging.error(f"An error occurred: {str(e)}")
