from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

# Initialize classes
db_connector = DatabaseConnector()
data_cleaning = DataCleaning()
data_extractor = DataExtractor()

# Initialize Database Engines
source_engine = db_connector.engine
local_engine = db_connector.local_engine

# List available tables in the source database
#available_tables = db_connector.list_db_tables(source_engine)

#if 'legacy_users' in available_tables:
    # Extract and clean legacy user data
    #legacy_users_data = data_extractor.read_rds_table(db_connector, 'legacy_users')
    #cleaned_user_data = data_cleaning.clean_user_data(legacy_users_data)
    #if cleaned_user_data is not None:
        #db_connector.upload_to_db(cleaned_user_data, 'dim_users', local_engine)
#else:
   # print("The table 'legacy_users' was not found in the source database.")

# Fetch PDF Data
#raw_card_data = data_extractor.retrieve_pdf_data()

# Check if raw_card_data is None
#if raw_card_data is not None:

    # Clean card data
    #cleaned_card_data = data_cleaning.clean_card_data(raw_card_data)

    # Upload to local database
    #if cleaned_card_data is not None:
        #db_connector.upload_to_db(cleaned_card_data, 'dim_card_details', engine=local_engine)
       # print("successful upload to postgreSQL")
#else:
    #print("Failed to retrieve or parse PDF data.")

# API Information
headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}"

# Step 1: List the number of stores
num_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint, headers)

# Step 2: Extract store data
store_data = data_extractor.retrieve_stores_data(store_details_endpoint, headers, num_stores)
print("successful")

# Step 3: Clean store data
cleaned_store_data = data_cleaning.clean_store_data(store_data)

# Step 4: Upload cleaned data to database
db_connector.upload_to_db(cleaned_store_data, 'dim_store_details', db_connector.local_engine)
