import pandas as pd
import numpy as np
import uuid
import re
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from geopy.geocoders import Nominatim



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
        df.replace(['NaN', 'NULL', 'NaT'], np.nan, inplace=True)
        df.dropna(how='all', inplace=True)
        
        if critical_columns:
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
        df = df.replace(['NaN', 'NULL', 'NaT'], np.nan)
        df = df.dropna()
        df = self.clean_letters(df, 'first_name')
        df = self.clean_letters(df, 'last_name')
        df = self.clean_letters(df, 'country')
        df = self.clean_country_code(df, 'country_code')
        df['date_of_birth'] = self.fixed_date(df, 'date_of_birth')
        df['join_date'] = self.fixed_date(df, 'join_date')
        df = self.clean_uuid(df, 'user_uuid')
        df = self.clean_phone_number(df, 'phone_number', 'country_code')
        df = self.clean_email(df, 'email_address')
        df = self.clean_address(df, 'address')
        return df

    def clean_store_data(self, df, critical_columns):
        """
        Clean store data in a DataFrame.
        :param df: DataFrame containing store data.
        :param critical_columns: Columns considered critical for cleaning.
        :return: Cleaned DataFrame.
        """
        df = df[df['store_code'] != 'NULL']
        df = self.clean_country_code(df, 'country_code')
        df['opening_date'] = self.fixed_date(df, 'opening_date')
        df = self.clean_letters(df, 'store_type')
        df = self.clean_letters(df, 'locality')
        df = self.clean_continent(df, 'continent')
        df = self.clean_latitude(df, 'latitude')
        df = self.clean_longitude(df, 'longitude')
        df = self.clean_numbers(df, 'staff_numbers')
        df = pd.DataFrame(df)
        df = self.clean_code(df, 'store_code')
        df = self.clean_address(df, 'address')
        df = self.drop_na_values(df, critical_columns)
        return df


    def clean_products_data(self, df, critical_columns):
        """
        Clean product data in a DataFrame.
        :param df: DataFrame containing product data.
        :param critical_columns: Columns considered critical for cleaning.
        :return: Cleaned DataFrame.
        """
        df = df.replace(['NaN', 'NULL', 'NaT'], np.nan)
        df = df.dropna()
        df = self.clean_price(df, 'product_price')
        df['date_added'] = self.fixed_date(df, 'date_added')
        df = self.clean_ean_number(df, 'EAN')
        df = self.convert_product_weights(df)
        df = self.drop_na_values(df, critical_columns)
        return df

    def clean_letters(self, df, column):
        """
        Clean columns with only letters.
        :param df: DataFrame containing letter columns.
        :param column: Column name of the letter columns.
        :return: DataFrame with cleaned letter columns.
        """
        df[column] = df[column].astype(str)
        return df[df[column].str.contains(r'^[a-zA-ZÄÖÜäöüßé\s\.\-\']+$', na=False)]
    
    def clean_country_code(self, df, column):
        """
        Clean country codes.
        :param df: DataFrame containing country codes.
        :param column: Column name of the country codes.
        :return: DataFrame with cleaned country codes.
        """
        df.loc[:, column] = df.loc[:, column].str.replace('GGB', 'GB')
        country_code_pattern = r'\b[A-Z]{2}\b'
        return df[df[column].str.match(country_code_pattern, na=False)]

    
    def clean_card_data(self, df):
        """
        Clean card data in a DataFrame.
        :param df: DataFrame containing card data.
        :return: Cleaned DataFrame.
        """
        df = df[df['card_number'] != 'NULL']
        df = self.clean_card_number(df, 'card_number')
        df['date_payment_confirmed'] = self.fixed_date(df, 'date_payment_confirmed')
        return df

    def clean_card_number(self, df, column):
        """
        Clean card numbers.
        :param df: DataFrame containing card numbers.
        :param column: Column name of the card numbers.
        :return: DataFrame with cleaned card numbers.
        """
        df.loc[:, column] = df.loc[:, column].astype(str)
        df.loc[:, column] = df.loc[:, column].str.replace('?', '')
        card_pattern = r'^[0-9]{9,20}'
        return df[df[column].str.match(card_pattern, na=False)]

    def fixed_date(self, df, column):
        """
        Fix and format dates in a DataFrame.
        :param df: DataFrame containing dates.
        :param column: Column name of the date.
        :return: DataFrame with formatted dates.
        """
        return pd.to_datetime(df[column], format='%Y-%m-%d', errors='coerce')


    def clean_phone_number(self, df, phone_column, country_code_column):
        '''
        To clean phone numbers
        Takes dataframe and column
        Return dataframe
        '''
        isd_code_map = {"GB": "+44", "DE": "+49", "US": "+1"}

        df[phone_column] = df[phone_column].astype(str)
        df[country_code_column] = df[country_code_column].astype(str)

        for index, row in df.iterrows():
            phone_number = row[phone_column]
            country_code = row[country_code_column]

            phone_number = re.sub(r'[\sx+()-.]','', phone_number)[:12]
        
            if len(phone_number) >= 10:
                if country_code in isd_code_map:
                    isd_code = isd_code_map[country_code]
                    if not phone_number.startswith(isd_code):
                        phone_number = isd_code + ' ' + phone_number
            else:
                phone_number = 'N/A'

            df.at[index, phone_column] = phone_number

        return df


    def clean_email(self, df, column):
        '''
        To clean emails
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = df[column].astype(str)
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
        return df[df[column].str.match(email_pattern, na=False)]
 
    def clean_address(self, df, column):
        '''
        To clean address
        Takes dataframe and column
        Return dataframe
        '''
        cleaned_df = df.copy()
        geolocator = Nominatim(user_agent="address_cleaner", timeout=100)

        for index, address in enumerate(cleaned_df[column]):
            if pd.notna(address):
                try:
                    location = geolocator.geocode(address, addressdetails=True, language="en")
                    if location and 'address' in location.raw:
                        address_parts = location.raw['address']
                        cleaned_address = ', '.join(filter(lambda x: x.strip(), [address_parts.get('road', ''), address_parts.get('house_number', ''), address_parts.get('city', ''), address_parts.get('state', ''), address_parts.get('postcode', ''), address_parts.get('country', '')]))
                        cleaned_df.at[index, column] = cleaned_address
                except (GeocoderTimedOut, GeocoderUnavailable) as e:
                    print(f"Geocoding error for address '{address}': {e}")

        return cleaned_df
        


    def clean_uuid(self, df, column):
        '''
        To clean uuid
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = df[column].astype(str)
        id_pattern = r'^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}'
        return df[df[column].str.match(id_pattern, na=False)]
    
    
    def clean_code(self, df, column):
        '''
        To clean store code
        Takes dataframe and column
        Return dataframe
        '''
        id_pattern = r'^[a-zA-Z]{2,3}-[a-zA-Z0-9]{6,8}'
        df[column] = np.where(df[column].str.match(id_pattern), df[column], np.nan)
        return df
    
    def clean_numbers(self, df, column):
        '''
        To clean data without numbers
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = pd.to_numeric(df[column], errors='coerce')
        return df
    

    def clean_continent(self, df, column):
        '''
        To clean continent column
        Takes dataframe and column
        Return dataframe
        '''
        continents = ['Asia', 'Africa', 'America', 'South America', 'Europe','Antarctica', 'Australia']
        df[column] = df[column].str.replace('eeEurope','Europe')
        df[column] = df[column].str.replace('eeAmerica','America')
        return df[df[column].isin(continents) | df[column].isnull()]
    
    def clean_latitude(self, df, column):
        '''
        To clean latitude
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = np.where((df[column] >= -90), df[column], np.nan )
        return df
    
    def clean_longitude(self, df, column):
        '''
        To clean longtitude
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = pd.to_numeric(df[column], errors='coerce')
        return df[(df[column] >= -180) & (df[column] <= 180) | pd.isna(df[column])]
    
    def clean_price(seld, df, column):
        '''
        To clean price data
        Takes dataframe and column
        Return dataframe
        '''
        pattern = r'^£(\d+(\.\d{2}))$'
        df[column] = np.where(df[column].str.match(pattern), df[column], np.nan)
        df = df.dropna(subset='product_price')

        return df

    def clean_ean_number(self, df, column):
        '''
        To clean ean numbers
        Takes dataframe and column
        Return dataframe
        '''
        df[column] = pd.to_numeric(df[column], errors='coerce')
        df[column] = np.where(len(df[column]) <= 13, df[column], np.nan)
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

    
