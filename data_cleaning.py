import pandas as pd
import logging
import re

logging.basicConfig(level=logging.INFO)

class DataCleaning:
    

    def clean_user_data(self, df):
        df.dropna(inplace=True)
        return df

    def clean_card_data(self, df):
        df.dropna(inplace=True)
        return df

    def clean_store_data(self, df):
        df.dropna(inplace=True)
        return df

    def convert_product_weights(self, df):
        def convert_weight(weight):
            weight = str(weight)
            if 'g' in weight:
                return float(re.sub('[^0-9.]', '', weight)) / 1000
            if 'ml' in weight:
                return float(re.sub('[^0-9.]', '', weight)) / 1000
            return weight

        df['weight'] = df['weight'].apply(convert_weight)
        return df
    
    def clean_products_data(self, df):
        df.dropna(inplace=True)
        return df
  
    