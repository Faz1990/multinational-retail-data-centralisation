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

    def list_number_of_stores(self, number_of_stores_endpoint, headers):
        response = requests.get(number_of_stores_endpoint, headers=headers)
        try:
            response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
            return response.json()['number_of_stores']
        except requests.exceptions.HTTPError as err:
            logging.error(f"HTTP error occurred: {err}")
        except KeyError:
            logging.error("Key 'number_of_stores' not found in API response.")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
        return None

    def retrieve_stores_data(self, store_details_endpoint, headers, num_stores):
        store_data_list = []
        for store_number in range(1, num_stores + 1):
            response = requests.get(store_details_endpoint.format(store_number=store_number), headers=headers)
            try:
                response.raise_for_status()  # Check for HTTP request errors
                store_data_list.append(response.json())
            except requests.exceptions.HTTPError as err:
                logging.error(f"HTTP error occurred: {err} - Store Number: {store_number}")
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e} - Store Number: {store_number}")
        return pd.DataFrame(store_data_list)


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

