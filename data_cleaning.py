"""The DataCleaning class contains methods designed to clean data from the various sources. 
Each method corrects different aspects of the data, such as correcting date fields,
filling in missing values, and standardising text formats. The methods are intended for use 
as part of a larger data processing pipeline, ensuring that the data is in a usable state 
before further analysis."""

import pandas as pd
import numpy as np
from datetime import datetime
from data_extraction import DataExtractor
import re


class DataCleaning:
    @staticmethod
    def _clean_user_data(df):
        """
        I use this method to tidy up users data. I ensure that NULL values are handled appropriately, 
        date errors are corrected, and incorrectly typed values are fixed. It's crucial for maintaining 
        data integrity and accuracy.
        """
        # I create a copy of the DataFrame to avoid changes to the original data
        cleaned_df = df.copy()

        # I remove rows with any NULL values
        cleaned_df = cleaned_df.dropna()

        # I correct any date errors, ensuring that 'date_of_birth' is accurate
        cleaned_df['date_of_birth'] = pd.to_datetime(cleaned_df['date_of_birth'], errors='coerce')
        # Rows with invalid dates are dropped to maintain data quality
        cleaned_df = cleaned_df.dropna(subset=['date_of_birth'])

        # Returning the cleaned DataFrame
        return cleaned_df
    
    def _clean_card_data(df):
        """
        I use this method to clean the card data. I also employ the masking of sensitive card
        numbers for security, standardise date formats, and removes any rows with dates that couldn't 
        be converted.
        """

        # I create a copy of the DataFrame to avoid changes to the original data
        cleaned_df = df.copy()

        # # Card numbers are masked for privacy, revealing only the last four digits
        # cleaned_df['card_number'] = (
        #     cleaned_df['card_number']
        #     .astype(str)
        #     .str.slice(-4)
        #     .apply(lambda x: f"XXXX-XXXX-XXXX-{x}")
        # )

        # Standardising and Validating Date Formats
        # First, convert the columns to datetime objects
        cleaned_df['expiry_date'] = pd.to_datetime(cleaned_df['expiry_date'], 
                                                   errors='coerce', format='%m/%y')
        cleaned_df['date_payment_confirmed'] = pd.to_datetime(cleaned_df['date_payment_confirmed'],
                                                               errors='coerce')

        # Removing non-numeric characters from card_number column

        cleaned_df['card_number']= cleaned_df['card_number'].astype('string')
        cleaned_df['card_number'] = cleaned_df['card_number'].str.replace('?', '')
        cleaned_df['card_number'] = cleaned_df['card_number'].where(cleaned_df['card_number'].str.contains(r'^\d+$'), np.nan)

       # Dates are standardised to a uniform format
        cleaned_df.loc[cleaned_df['expiry_date'].notnull(),
                        'expiry_date'] = cleaned_df['expiry_date'].dt.strftime('%Y-%m-%d')
        cleaned_df.loc[cleaned_df['date_payment_confirmed'].notnull(),
                        'date_payment_confirmed'] = cleaned_df['date_payment_confirmed'].dt.strftime('%Y-%m-%d')

        # Dates that could not be converted are dropped to ensure consistency
        cleaned_df = cleaned_df.dropna(subset=['expiry_date', 'date_payment_confirmed'])

        # 'card_provider' field converted to title case for uniformity
        cleaned_df['card_provider'] = cleaned_df['card_provider'].str.title()

        # Any whitespace is removed from string columns to clean the data further
        for col in cleaned_df.select_dtypes(include=['object']):
            cleaned_df[col] = cleaned_df[col].str.strip()

        # # Remaining NaNs are filled with a placeholder value to avoid processing errors
        # cleaned_df.fillna('Unknown', inplace=True)

        return cleaned_df

    @staticmethod
    def _convert_product_weights(df):
        """
        I use this method to standardise product weight measurements across the dataset.
        It ensures all weights are in kilograms, facilitating consistent analysis.
        """
        products_df = df.copy()

        def convert_to_kg(weight):
            # I ensure the weight is a string before extracting numeric values
            weight_str = str(weight)
            # Extracting the numeric value and units
            value_str = ''.join(filter(str.isdigit, weight_str)) or '0'
            units = ''.join(filter(str.isalpha, weight_str))
            value = float(value_str)
            
            # Conversion is done from grams to kilograms when necessary
            return value / 1000 if units in ['ml', 'g'] else value
            
        # I apply the conversion across the weight column
        products_df['weight'] = products_df['weight'].apply(convert_to_kg)

        return products_df

    @staticmethod
    def clean_store_data(df):
        """
        I use this method to clean the store data DataFrame, ensuring it's free from null values,
        and the store types are capitalised appropriately. It's a key step in data preprocessing.
        """
        cleaned_df = df.copy()

        # I remove columns that are completely null, which are uninformative
        cleaned_df = cleaned_df.dropna(axis=1, how='all')

        # Store opening dates are converted to a consistent datetime format
        cleaned_df['opening_date'] = pd.to_datetime(cleaned_df['opening_date'], errors='coerce')
        cleaned_df.dropna(subset=['opening_date'], inplace=True)  # Drop rows where opening_date was invalid

        # Removing rows with null 'store_type' values 
        cleaned_df = cleaned_df.dropna(subset=['store_type'])

        # Removing rows with null 'store_code' values 

        cleaned_df = cleaned_df.dropna(subset=['store_code'])

         # I remove unwanted columns that may have been added erroneously
        cleaned_df.drop(columns=["lat"], errors='ignore', inplace=True)

        # The store type is capitalised to maintain a standard naming convention
        cleaned_df['store_type'] = cleaned_df['store_type'].str.title()

        # I ensure text fields are also capitalised for consistency
        cleaned_df['locality'] = cleaned_df['locality'].str.title()
        cleaned_df['store_type'] = cleaned_df['store_type'].str.title()

        # I correct any prefixed 'ee' in the continent names
        cleaned_df['continent'] = cleaned_df['continent'].str.replace(r'^ee', '', regex=True)

        # Any remaining NaNs are replaced with a placeholder to keep the DataFrame complete
        cleaned_df.fillna('Unknown', inplace=True)

        # Latitude and longitude data types are enforced to be numeric
        cleaned_df['latitude'] = pd.to_numeric(cleaned_df['latitude'], errors='coerce')
        cleaned_df['longitude'] = pd.to_numeric(cleaned_df['longitude'])

        # Cleaning staff_numbers, removing any letters and keeping only numbers
        cleaned_df['staff_numbers'] = cleaned_df['staff_numbers'].astype(str).apply(lambda x: ''.join(filter(str.isdigit, x)))

        # Converting cleaned numbers back to integers
        cleaned_df['staff_numbers'] = pd.to_numeric(cleaned_df['staff_numbers'], errors='coerce')

        # Handling possible NaNs after conversion (if any non-numeric value was present)
        cleaned_df['staff_numbers'] = cleaned_df['staff_numbers'].fillna(0).astype(int)
        
        # Removing rows with null 'locality' values from the store details.
        cleaned_df = cleaned_df.dropna(subset=['locality'])
        
        return cleaned_df
    

    @staticmethod
    def clean_products_data(df):
            """
            I use this method to clean and standardise the product data. It involves various steps to
             ensure that the data is uniform and ready for analysis.
            """
            # Creating copy of the DataFrame to avoid changes to the original data
            cleaned_df = df.copy()

            # Trimming any leading or trailing whitespace from all string columns to ensure consistency
          
            for col in cleaned_df.select_dtypes(include=['object']):
                cleaned_df[col] = cleaned_df[col].str.strip()

            # Category is capitalised to maintain a standard naming convention
            cleaned_df['category'] = cleaned_df['category'].str.title()

            # Removing duplicates
            cleaned_df.drop_duplicates(inplace=True)

            # Checking for unique constraints
            if cleaned_df['EAN'].duplicated().any():
                # Handling duplicates in 'EAN' column
                pass  

            # Converting product codes to uppercase to maintain consistency across the dataset.
            cleaned_df['product_code'] = cleaned_df['product_code'].astype(str)
            # Converting the 'product_code' column to uppercase
            cleaned_df['product_code'] = cleaned_df['product_code'].str.upper()
            # Handling NaN values after conversion, if there were any
            cleaned_df['product_code'] = cleaned_df['product_code'].replace('nan', np.nan)

            # Changing the name of the 'Unnamed: 0' column to 'index' for consistency in other tables..
            cleaned_df.rename(columns={'Unnamed: 0': 'index'}, inplace=True)

            # Replacing any underscores and correcting typos in the 'removed' column.
            cleaned_df['removed'] = cleaned_df['removed'].str.replace("Still_avaliable", "Still available")

            # Replacing dashes with spaces in the 'category' column
            cleaned_df['category'] = cleaned_df['category'].str.replace('-', ' ', regex=False)

            # Converting to datetime
            cleaned_df['date_added'] = pd.to_datetime(cleaned_df['date_added'], errors='coerce')

            # Dropping rows with missing 'date_added'
            cleaned_df = cleaned_df.dropna(subset=['date_added'])

            # Ensuring that 'weight' values are non-negative and numeric.
            cleaned_df['weight'] = pd.to_numeric(cleaned_df['weight'], errors='coerce')
            cleaned_df = cleaned_df[cleaned_df['weight'] >= 0]

            # Filling any remaining NaN values with a placeholder 'Unknown'.
            cleaned_df = cleaned_df.fillna('Unknown')
            
            # Returning the cleaned and standardised dataframe.
            return cleaned_df
    

    @staticmethod
    def clean_orders_data(df):
        """
        I use this method to clean the orders data, including handling UUIDs and standardising text fields.
        """

        # Making a copy of the DataFrame to avoid modifying the original data
        cleaned_df = df.copy()
        
        # Defining the UUID regex pattern
        uuid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

        # Dropping columns that are not required for analysis.
        cleaned_df.drop(columns=["1", "first_name", "last_name", "level_0"], errors='ignore', inplace=True)

        # Applying UUID regex pattern to validate 'user_uuid' and 'date_uuid' columns
        cleaned_df['user_uuid'] = cleaned_df['user_uuid'].apply(lambda x: x if x and uuid_pattern.match(x) else None)
        cleaned_df['date_uuid'] = cleaned_df['date_uuid'].apply(lambda x: x if x and uuid_pattern.match(x) else None)

        # Removing rows with null 'store_code' values
        cleaned_df = cleaned_df.dropna(subset=['store_code'])
    
        # Ensuring product_code is a string and then converting to uppercase for consistency
        cleaned_df['product_code'] = cleaned_df['product_code'].astype(str).str.upper()
        # Handling NaN values after conversion, if there were any
        cleaned_df['product_code'] = cleaned_df['product_code'].replace('nan', np.nan)


        # Filling NaN values with an appropriate placeholder based on the column data type
        for col in cleaned_df.columns:
            if cleaned_df[col].dtype == np.float64 or cleaned_df[col].dtype == np.int64:
                cleaned_df[col].fillna(0, inplace=True)  # Replacing NaN with 0 for numerical columns
            elif cleaned_df[col].dtype == 'datetime64[ns]':
                cleaned_df[col].fillna(pd.NaT, inplace=True)  # Replacing NaN with NaT for datetime columns
            else:
                cleaned_df[col].fillna('Unknown', inplace=True)  # Replacing NaN with 'Unknown' for object (string) columns

        # Returning the cleaned DataFrame
        return cleaned_df
    

    @staticmethod
    def clean_date_events_data(df):
        """
        I use this method to clean the date events DataFrame by standardising date formats and cleaning text fields.
        """
        cleaned_df = df.copy() # Copying the dataframe to avoid modifying the original data.

        # Converting 'timestamp' to a standard datetime format for consistency and ease of analysis.
        cleaned_df['timestamp'] = pd.to_datetime(cleaned_df['timestamp'], format='mixed', errors='coerce')
        
        # Replacing underscores with spaces in the 'time_period' column for better readability.
        # Standardising the text to ensure uniformity across the dataset.

        # Droing rows where 'timestamp' is null
        cleaned_df = cleaned_df.dropna(subset=['timestamp'])
        
        time_periods = {
            'Morning': 'Morning',
            'Evening': 'Evening',
            'Midday': 'Midday',
            'Late_Hours': 'Late Hours'  # Replacing underscores with space
        }
        cleaned_df['time_period'] = cleaned_df['time_period'].replace(time_periods)
        
        # Returning the cleaned and uniform dataframe.
        return cleaned_df