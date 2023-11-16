import pandas as pd
import logging
import re

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
    def convert_product_weights(weight):
        """Converts weights to kilograms."""
        if isinstance(weight, str):
            # Remove non-numeric characters and convert to float
            numeric_weight = float(re.sub('[^0-9.]', '', weight))

            if 'g' in weight or 'ml' in weight:
                # Convert grams or milliliters to kilograms
                return numeric_weight / 1000
        # Return the weight if it's not a string or doesn't contain 'g' or 'ml'
        return weight

    def clean_user_data(self, df):
        df = self.drop_na_values(df)
        df = self.clean_column_names(df)
        
        return df

    def clean_card_data(self, df):
        if df is None:
            logging.warning("DataFrame is None. Skipping cleaning.")
            return df

        df = self.drop_na_values(df)
        df = self.clean_column_names(df)
        return df

    def clean_store_data(self, df, critical_columns):
        df = self.drop_na_values(df, critical_columns)
        return df
        
        

    def clean_products_data(self, df, critical_columns):
        df = self.drop_na_values(df, critical_columns)
        return df
    
    @staticmethod
    def clean_orders_data(df):
        df.drop(columns=['first_name', 'last_name', '1'], inplace=True, errors='ignore')
        return df

  
    