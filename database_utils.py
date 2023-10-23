import yaml
from sqlalchemy import create_engine

class DatabaseConnector:

    def __init__(self):
        pass  # Any initialization tasks can go here

    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            return yaml.safe_load(file)

    def init_db_engine(self):
        creds = self.read_db_creds()
        return create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")

    def list_db_tables(self):
        engine = self.init_db_engine()
        return engine.table_names()
