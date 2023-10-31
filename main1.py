from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

# Initialize classes
db_connector = DatabaseConnector()
data_cleaning = DataCleaning()
data_extractor = DataExtractor()

# List available tables in the database
available_tables = db_connector.list_db_tables()  


if 'legacy_users' in available_tables:
    
    # Extract Data from 'legacy_users' into DataFrame
    legacy_users_data = data_extractor.read_rds_table(db_connector, 'legacy_users')
    
    # Clean the user data
    cleaned_user_data = data_cleaning.clean_user_data(legacy_users_data)
    
    # Upload cleaned data to new table 'dim_users'
    if cleaned_user_data is not None:
        db_connector.upload_to_db(cleaned_user_data, 'dim_users')

else:
    print("The table 'legacy_users' was not found in the database.")


