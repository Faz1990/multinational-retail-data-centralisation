import pandas as pd
import logging
import re
import uuid

logging.basicConfig(level=logging.INFO)

class DataCleaning:
    """
    This class contains methods for cleaning different types of data.
    It includes functions for handling missing values, standardizing column names,
    converting weights, and more.
    """

    @staticmethod
    def drop_na_values(df, critical_columns=None):
        """
        Drop rows with missing values.
        :param df: DataFrame to clean.
        :param critical_columns: Columns considered critical where missing values are not allowed.
        :return: Cleaned DataFrame.
        """
        # Dropping rows where all elements are missing.
        df.dropna(how='all', inplace=True)
        
        if critical_columns:
            # Dropping rows with missing values in critical columns
            df.dropna(subset=critical_columns, inplace=True)
        return df

    @staticmethod
    def clean_column_names(df):
        """
        Standardize DataFrame column names.
        :param df: DataFrame with columns to standardize.
        :return: DataFrame with standardized column names.
        """
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
        return df

    @staticmethod
    def convert_to_kg(weight):
        """
        Converts various weight units to kilograms.
        Assumes 1 ml = 1 g, 1000 g = 1 kg.
        :param weight: Weight value to convert.
        :return: Weight in kilograms.
        """
        # Implementation of the conversion logic
        # ...

    @staticmethod
    def convert_product_weights(df):
        """
        Convert product weights to a standard unit (kilograms).
        :param df: DataFrame containing product weights.
        :return: DataFrame with weights converted to kilograms.
        """
        df['weight'] = df['weight'].apply(DataCleaning.convert_to_kg)
        return df
    
    @staticmethod
    def is_valid_uuid(uuid_to_test, version=4):
        """
        Check if a string is a valid UUID.
        :param uuid_to_test: String to test.
        :param version: UUID version to validate against.
        :return: Boolean indicating if the string is a valid UUID.
        """
        try:
            uuid_obj = uuid.UUID(uuid_to_test, version=version)
            return str(uuid_obj) == uuid_to_test.lower()
        except ValueError:
            return False

    def clean_user_data(self, df):
        """
        Clean user data in a DataFrame.
        :param df: DataFrame containing user data.
        :return: Cleaned DataFrame.
        """
        # Implementation of user data cleaning logic
        # ...

    def clean_card_data(self, df):
        """
        Clean card data in a DataFrame.
        :param df: DataFrame containing card data.
        :return: Cleaned DataFrame.
        """
        if df is None:
            logging.warning("DataFrame is None. Skipping cleaning.")
            return df

        df = self.drop_na_values(df)
        df = self.clean_column_names(df)
        return df

    def clean_store_data(self, df, critical_columns):
        """
        Clean store data in a DataFrame.
        :param df: DataFrame containing store data.
        :param critical_columns: Columns considered critical for cleaning.
        :return: Cleaned DataFrame.
        """
        # Implementation of store data cleaning logic
        # ...

    def clean_products_data(self, df, critical_columns):
        """
        Clean product data in a DataFrame.
        :param df: DataFrame containing product data.
        :param critical_columns: Columns considered critical for cleaning.
        :return: Cleaned DataFrame.
        """
        df = self.drop_na_values(df, critical_columns)
        return df
    
    @staticmethod
    def clean_orders_data(df):
        """
        Clean order data in a DataFrame.
        :param df: DataFrame containing order data.
        :return: Cleaned DataFrame.
        """
        df.drop(columns=['first_name', 'last_name', '1'], inplace=True, errors='ignore')
        return df

    @staticmethod
    def clean_date_details_data(df):
        """
        Clean date details in a DataFrame.
        :param df: DataFrame containing date details.
        :return: Cleaned DataFrame.
        """
        df.dropna(inplace=True)  # Remove rows with null values
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')  # Standardize column names
        return df
