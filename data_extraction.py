import requests
import pandas as pd
import logging
import boto3
import tabula
import time

logging.basicConfig(level=logging.INFO)

class DataExtractor:
    """
    This class provides methods for extracting data from various sources 
    including relational databases, APIs, S3 buckets, and PDF files.
    """

    def __init__(self):
        """
        Initializes the DataExtractor class.
        """
        pass

    def list_db_tables(self, engine):
        """
        Lists all tables in a database.
        :param engine: SQLAlchemy engine connected to the database.
        :return: List of table names.
        """
        return engine.table_names()

    def read_rds_table(self, engine, table_name):
        """
        Reads a table from a relational database service (RDS).
        :param engine: SQLAlchemy engine connected to the database.
        :param table_name: Name of the table to read.
        :return: DataFrame containing the table data.
        """
        return pd.read_sql_table(table_name, engine)

    def list_number_of_stores(self, api_endpoint, headers):
        """
        Retrieves the number of stores from an API endpoint.
        :param api_endpoint: URL of the API endpoint.
        :param headers: Dictionary containing HTTP headers for the request.
        :return: Number of stores or 0 in case of an error.
        """
        response = requests.get(api_endpoint, headers=headers)
        if response.ok:
            try:
                return response.json()['number_stores']
            except KeyError as e:
                logging.error(f"Key error: {e} not found in API response.")
                return 0
        else:
            logging.error(f"API call failed with status code: {response.status_code}")
            response.raise_for_status()

    def retrieve_stores_data(self, api_endpoint, headers, num_stores):
        """
        Retrieves data for each store from an API endpoint.
        :param api_endpoint: URL of the API endpoint.
        :param headers: Dictionary containing HTTP headers for the request.
        :param num_stores: Total number of stores to retrieve.
        :return: DataFrame containing data of all stores.
        """
        store_data_list = []
        for store_number in range(1, num_stores + 1):
            store_url = api_endpoint.format(store_number=store_number)
            for attempt in range(3):  # Try up to 3 times for each store
                try:
                    response = requests.get(store_url, headers=headers)
                    response.raise_for_status()
                    store_data_list.append(response.json())
                    break  # Success, so break out of the retry loop
                except requests.exceptions.HTTPError as err:
                    logging.error(f"Attempt {attempt+1} - HTTP error for store {store_number}: {err}")
                    if response.status_code == 500:
                        time.sleep(1)  # Wait for 1 second before retrying
                    else:
                        break  # For non-500 errors, break the loop without retrying
                except Exception as e:
                    logging.error(f"Attempt {attempt+1} - Unexpected error for store {store_number}: {e}")
                    break  # Break on non-HTTP errors
        return pd.DataFrame(store_data_list)

    def extract_from_s3(self, s3_url):
        """
        Extracts data from an S3 bucket.
        :param s3_url: URL of the S3 bucket.
        :return: DataFrame containing data from the S3 bucket.
        """
        s3 = boto3.client('s3')
        bucket_name = 'data-handling-public'
        object_key = 'products.csv'
        s3.download_file(bucket_name, object_key, 'local_products.csv')
        df = pd.read_csv('local_products.csv')
        return df

    def retrieve_pdf_data(self, link="https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"):
        """
        Retrieves data from a PDF file.
        :param link: URL of the PDF file.
        :return: DataFrame containing data extracted from the PDF.
        """
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

    def extract_json_from_s3(self, json_url):
        """
        Extract JSON data from an S3 bucket.
        :param json_url: S3 bucket URL to the JSON file.
        :return: JSON data or None on error.
        """
        response = requests.get(json_url)
        if response.ok:
            return response.json()
        else:
            logging.error(f"Failed to download JSON data. HTTP Status Code: {response.status_code}")
            return None
