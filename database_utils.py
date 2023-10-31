import yaml
from sqlalchemy import create_engine, text, MetaData
import pandas as pd
import logging
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO)

class DatabaseConnector:
    def __init__(self):
        self.engine = self.init_db_engine()
        self.local_engine = self.init_local_db_engine()

    def read_db_creds(self, filepath='db_creds.yaml'):
        with open(filepath, 'r') as file:
            return yaml.safe_load(file)

    def init_db_engine(self):
        creds = self.read_db_creds()
        engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    def init_local_db_engine(self):
        username = "postgres"
        password = "Tekken56"
        host = "localhost"
        port = "5432"  # default PostgreSQL port
        db_name = "sales_data"
        local_engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{db_name}")
        return local_engine

    def list_db_tables(self, engine):  # Added engine parameter
        meta = MetaData()
        meta.reflect(bind=engine)  
        return [table.name for table in meta.tables.values()]

    def upload_to_db(self, df, table_name, engine):  # Added engine parameter
        try:
            with engine.connect() as connection:  
                with connection.begin() as transaction:  
                    df.to_sql(table_name, connection, if_exists='replace')
                    transaction.commit()  
        except SQLAlchemyError as e:  
            logging.error(f"An SQLAlchemy error occurred: {str(e)}")
        except Exception as e:  
            logging.error(f"An error occurred: {str(e)}")

