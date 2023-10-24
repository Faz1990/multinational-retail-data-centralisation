import yaml
from sqlalchemy import create_engine, text
import pandas as pd
import logging


logging.basicConfig(level=logging.INFO)

class DatabaseConnector:
    def __init__(self):
        pass

    def read_db_creds(self, filepath='db_creds.yaml'):
        with open(filepath, 'r') as file:
            return yaml.safe_load(file)

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self, engine):
        return engine.table_names()

    def upload_to_db(self, df, table_name):
        engine = self.init_db_engine()
        connection = engine.connect()  # Create a connection object

        try:
            # Start a new transaction and set it to read-write mode
            connection.execute(text("BEGIN;"))
            connection.execute(text("SET TRANSACTION READ WRITE;"))
            
            # Upload DataFrame to SQL table
            df.to_sql(table_name, engine, if_exists='replace')

            # Commit the transaction
            connection.execute(text("COMMIT;"))
        except Exception as e:
            # In case of error, rollback the transaction
            connection.execute(text("ROLLBACK;"))
            print(f"An error occurred: {e}")
        finally:
            # Close the connection
            connection.close()
