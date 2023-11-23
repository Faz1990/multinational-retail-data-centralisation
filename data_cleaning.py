import pandas as pd
import logging
import re
import uuid

logging.basicConfig(level=logging.INFO)

class DataCleaning:

    @staticmethod
    def drop_na_values(df, critical_columns=None):
        # Dropping rows where all elements are missing.
        df.dropna(how='all', inplace=True)
        
        if critical_columns:
            df.dropna(subset=critical_columns, inplace=True)
        return df

    @staticmethod
    def clean_column_names(df):
        
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
        return df

    @staticmethod
    def convert_to_kg(weight):
        """
        Converts weight to kilograms.
        Assumes 1 ml = 1 g, 1000 g = 1 kg.
        """
        if pd.isna(weight):
            return None
        if isinstance(weight, float):
            # Assuming the weight is already in kilograms
            return weight
        if isinstance(weight, str):
            # Remove non-numeric characters except for the decimal point
            numeric_weight = float(''.join(filter(lambda x: x.isdigit() or x == '.', weight)))

            # Convert to kilograms if needed
            if 'ml' in weight or 'g' in weight:
                return numeric_weight / 1000  # Convert grams or milliliters to kilograms
            elif 'kg' in weight:
                return numeric_weight  # Already in kilograms
            else:
                # Handle other units as needed
                pass
        else:
            # Handle non-string, non-float types
            return None

    @staticmethod
    def convert_product_weights(df):
        """
        Cleans up the weight column and converts weights to kilograms.
        """
        df['weight'] = df['weight'].apply(DataCleaning.convert_to_kg)
        return df
    
    @staticmethod
    def is_valid_uuid(uuid_to_test, version=4):
        try:
            uuid_obj = uuid.UUID(uuid_to_test, version=version)
            return str(uuid_obj) == uuid_to_test.lower()
        except ValueError:
            return False

    def clean_user_data(self, df):
        # Drop rows based on critical columns
        critical_columns = ['index', 'user_uuid']
        df = df.dropna(subset=critical_columns)

        # Correcting date formats with handling of NaT values
        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce')
        df = df.dropna(subset=['date_of_birth', 'join_date'])

        # Truncate country_code and handle UUIDs
        df['country_code'] = df['country_code'].apply(lambda x: x[:3] if pd.notnull(x) else x)
        df['user_uuid'] = df['user_uuid'].apply(
            lambda x: x if self.is_valid_uuid(x) else str(uuid.uuid4())
    )

        # Final check for NULL values
        print(df.isnull().sum())

        return df
        

    def clean_card_data(self, df):
        if df is None:
            logging.warning("DataFrame is None. Skipping cleaning.")
            return df

        df = self.drop_na_values(df)
        df = self.clean_column_names(df)
        return df

    def clean_store_data(self, df, critical_columns):
        # Convert textual 'NULL' to actual NULL values
        df.replace('NULL', pd.NA, inplace=True)

        # Handle non-numeric data in 'longitude' and 'latitude'
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')

        # Convert 'opening_date' to datetime or set to NULL if invalid
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')

        # Ensure 'staff_numbers' fit within SMALLINT range
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], downcast='integer', errors='coerce')

        return df
        
        

    def clean_products_data(self, df, critical_columns):
        df = self.drop_na_values(df, critical_columns)
        return df
    
    @staticmethod
    def clean_orders_data(df):
        df.drop(columns=['first_name', 'last_name', '1'], inplace=True, errors='ignore')
        return df

    @staticmethod
    def clean_date_details_data(df):
        df.dropna(inplace=True)  # Remove rows with null values
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')  # Standardize column names
        return df
    