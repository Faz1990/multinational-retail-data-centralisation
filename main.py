from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

# Initialize instances
db_connector = DatabaseConnector()
data_extractor = DataExtractor()
data_cleaning = DataCleaning()

# API Details
headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
num_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"

# Get Number of Stores
num_stores = data_extractor.list_number_of_stores(num_stores_endpoint, headers)

# Extract Store Data
if num_stores:
    store_data = data_extractor.retrieve_stores_data(store_details_endpoint, headers, num_stores)
    if store_data is not None:
        # Clean Store Data
        cleaned_store_data = data_cleaning._clean_store_data(store_data)
        if cleaned_store_data is not None:
            # Upload to DB
            db_connector.upload_to_db(cleaned_store_data, 'dim_store_details')
