import pandas as pd
import logging
import re

logging.basicConfig(level=logging.INFO)

class DataCleaning:

    @staticmethod
    def drop_na_values(df):
        # Dropping rows where all elements are missing.
        df.dropna(how='all', inplace=True)
        # Assuming some columns are critical and cannot have NaNs, they are dropped.
        # Replace 'critical_column' with the actual critical column names.
        critical_columns = ['critical_column1', 'critical_column2']
        df.dropna(subset=critical_columns, inplace=True)
        return df

    @staticmethod
    def clean_column_names(df):
        # Clean column names to ensure consistency.
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
        return df

    @staticmethod
    def convert_weights_to_kg(weight):
        # Helper function to convert weight to kilograms.
        if isinstance(weight, str):
            if 'g' in weight:
                return float(re.sub('[^0-9.]', '', weight)) / 1000
            elif 'ml' in weight:
                return float(re.sub('[^0-9.]', '', weight)) / 1000
        return weight

    def clean_user_data(self, df):
        df = self.drop_na_values(df)
        df = self.clean_column_names(df)
        # Add more user-specific cleaning as needed.
        return df

    def clean_card_data(self, df):
        if df is None:
            logging.warning("DataFrame is None. Skipping cleaning.")
            return df

        df = self.drop_na_values(df)
        df = self.clean_column_names(df)
        # Add more card-specific cleaning as needed.
        return df

    def clean_store_data(self, df):
        df = self.drop_na_values(df)
        df = self.clean_column_names(df)
        # Add more store-specific cleaning as needed.
        return df

    def clean_products_data(self, df):
        df = self.drop_na_values(df)
        df = self.clean_column_names(df)
        df['weight'] = df['weight'].apply(self.convert_weights_to_kg)
        # Add more product-specific cleaning as needed.
        return df

  
    