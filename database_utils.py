import yaml
from sqlalchemy import create_engine, text, MetaData
import pandas as pd
import logging
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO)

class DatabaseConnector:
    def __init__(self):
        self.engine = self.init_db_engine()

    def read_db_creds(self, filepath='db_creds.yaml'):
        with open(filepath, 'r') as file:
            return yaml.safe_load(file)

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self):  # Removed 'engine' parameter
        meta = MetaData()
        meta.reflect(bind=self.engine)  # Use 'self.engine' directly
        return [table.name for table in meta.tables.values()]

    def upload_to_db(self, df, table_name):
        try:
            with self.engine.connect() as connection:  # using context manager for connection
                with connection.begin() as transaction:  # explicit transaction
                    df.to_sql(table_name, connection, if_exists='replace')
                    transaction.commit()  # explicit commit
        except SQLAlchemyError as e:  # catching all SQLAlchemy errors
            logging.error(f"An SQLAlchemy error occurred: {str(e)}")
        except Exception as e:  # general exception
            logging.error(f"An error occurred: {str(e)}")
