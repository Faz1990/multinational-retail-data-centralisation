from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
from data_extraction import DataExtractor

# Initialize instances
data_extractor = DataExtractor()
data_cleaning = DataCleaning()
db_connector = DatabaseConnector()

# Reading from RDS
user_data = data_extractor.read_rds_table(db_connector, 'your_table_name')
if user_data is not None:
    cleaned_user_data = data_cleaning.clean_user_data(user_data)
    if cleaned_user_data is not None:
        db_connector.upload_to_db(cleaned_user_data, 'dim_users')

# Reading from PDF (Replace with your actual PDF link)
pdf_link = "https://your_pdf_link_here"
card_data = data_extractor.retrieve_pdf_data(pdf_link)
if card_data is not None:
    cleaned_card_data = data_cleaning.clean_card_data(card_data)
    if cleaned_card_data is not None:
        db_connector.upload_to_db(cleaned_card_data, 'dim_card_details')
