import requests
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

class DataExtractor:
    def __init__(self):
        pass

    def list_db_tables(self, engine):
        return engine.table_names()

    def read_rds_table(self, db_connector_instance, table_name):
        engine = db_connector_instance.init_db_engine()
        return pd.read_sql_table(table_name, engine)

    def list_number_of_stores(self, api_endpoint, headers):
        response = requests.get(api_endpoint, headers=headers)
        return response.json()['number_of_stores']

    def retrieve_stores_data(self, api_endpoint, headers, num_stores):
        store_list = []
        for i in range(1, num_stores + 1):
            response = requests.get(f"{api_endpoint}/{i}", headers=headers)
            store_list.append(response.json())
        return pd.DataFrame(store_list)


