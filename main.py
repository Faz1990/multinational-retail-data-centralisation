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
#headers = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
#number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
#store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"

# Step 1: List the number of stores
#num_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint, headers)

#if num_stores > 0:
    # Step 2: Extract store data
    #store_data = data_extractor.retrieve_stores_data(store_details_endpoint, headers, num_stores)
    #print("Successful data extraction.")
    
    # Inspect the DataFrame columns
    #print("DataFrame columns:", store_data.columns)

    # Step 3: Clean store data
    #critical_columns = ['address', 'store_code'] 
    #cleaned_store_data = data_cleaning.clean_store_data(store_data, critical_columns)

    # Step 4: Upload cleaned data to database
    #db_connector.upload_to_db(cleaned_store_data, 'dim_store_details', local_engine)
#else:
    #print("No stores to retrieve based on the API response.")

# Extract data from S3
#s3_url = "s3://data-handling-public/products.csv"
#product_data = data_extractor.extract_from_s3(s3_url)
#print("successful Extraction")

# Convert product weights
#product_data = data_cleaning.convert_product_weights(product_data)
#print("Successful Conversion")

# Clean products data
#critical_columns_product = ['product_name', 'product_price', 'category']
#cleaned_product_data = data_cleaning.clean_products_data(product_data, critical_columns_product)
#print("Products Successfully cleaned")

# Step 4: Upload cleaned product data to database
#db_connector.upload_to_db(cleaned_product_data, 'dim_products', local_engine)
#print("Successful Upload")

#orders_table_name = "orders_table"  
#orders_df = data_extractor.read_rds_table(db_connector.engine, orders_table_name)
#print("Data Extracted Successfully")

# Clean the orders data
#cleaned_orders_df = DataCleaning.clean_orders_data(orders_df)
#print("Data Cleaned Successfully")

#print("Columns before resetting index:", cleaned_orders_df.columns)

#columns_to_drop = [col for col in ['level_0', 'index'] if col in cleaned_orders_df.columns]
#if columns_to_drop:
    #cleaned_orders_df.drop(columns=columns_to_drop, inplace=True)


#print("Columns after resetting index:", cleaned_orders_df.columns)

#db_connector.upload_to_db(cleaned_orders_df, 'orders_table', local_engine)
#print("Successful Upload")

# Extract JSON data from S3
json_url = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
date_details_data = data_extractor.extract_json_from_s3(json_url)

if date_details_data is not None:
    print("JSON Data Extracted Successfully")

    # Convert JSON data to DataFrame
    date_details_df = pd.DataFrame(date_details_data)

    # Clean the date details data
    cleaned_date_details_df = DataCleaning.clean_date_details_data(date_details_df)
    print("Date Details Data Cleaned Successfully")

    # Upload to the database
    db_connector.upload_to_db(cleaned_date_details_df, 'dim_date_times', db_connector.engine)
    print("Date Details Data Successfully Uploaded")
else:
    print("Failed to extract JSON data.")