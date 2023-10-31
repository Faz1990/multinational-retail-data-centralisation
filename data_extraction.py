import requests
import pandas as pd
import logging
import boto3
import tabula

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
        try:
            return response.json()['number_of_stores']
        except KeyError:
            logging.warning("Key 'number_of_stores' not found in API response.")
            return None

    def retrieve_stores_data(self, api_endpoint, headers, num_stores):
        store_list = []
        for i in range(1, num_stores + 1):
            response = requests.get(f"{api_endpoint}/{i}", headers=headers)
            store_list.append(response.json())
        return pd.DataFrame(store_list)

    def extract_from_s3(self, s3_url):
        s3 = boto3.client('s3')
        bucket_name = 'data-handling-public'
        object_key = 'products.csv'
        s3.download_file(bucket_name, object_key, 'local_products.csv')
        df = pd.read_csv('local_products.csv')
        return df

    # Retrieve PDF Data
    def retrieve_pdf_data(self, link="https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"):
        try:
            response = requests.get(link)
            if response.status_code == 200:
                with open("local_file.pdf", "wb") as f:
                    f.write(response.content)

                dfs = tabula.read_pdf("local_file.pdf", pages='all', multiple_tables=True)
                merged_df = pd.concat(dfs, ignore_index=True)
                
                return merged_df
            else:
                logging.warning(f"Failed to download PDF. HTTP Status Code: {response.status_code}")
                return None
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            return None

