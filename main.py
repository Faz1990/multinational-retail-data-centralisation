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

# Step 1: Extract from S3
s3_url = 's3://data-handling-public/products.csv'
products_df = data_extractor.extract_from_s3(s3_url)

# Step 2: Convert weights
products_df = data_cleaning.convert_product_weights(products_df)

# Step 3: Clean product data
products_df = data_cleaning.clean_products_data(products_df)

# Step 4: Upload to DB
db_connector.upload_to_db(products_df, 'dim_products')