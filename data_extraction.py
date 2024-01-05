"""
The DataExtractor class serves as a utility to facilitate data extraction from various sources such as
CSV files, S3 buckets, and APIs. Its methods simplify the process of data retrieval, transforming 
raw data into pandas DataFrames ready for further processing.
"""

import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from database_utils import DatabaseConnector
import tabula
import requests
import boto3
from io import StringIO, BytesIO

class DataExtractor:
    @staticmethod
    def read_rds_table(db_connector, table_name):
        """
        This method retrieves data from a specified RDS table. It uses a DatabaseConnector instance to
        establish a connection and then extracts the data into a pandas DataFrame. This function is 
        essential for acquiring initial data sets for analysis.
        """
        try:
            # I am using a private method from db_connector to securely initialise the database engine
            engine = db_connector._init_db_engine()
            engine = db_connector._init_db_engine()
            with engine.connect() as connection:
                dataframe = pd.read_sql_table(table_name, connection)
            return dataframe
        except SQLAlchemyError as e:
            print(f"An error occurred while extracting data: {e}")
            return None

    def retrieve_pdf_data(pdf_link):
        """
        This function uses the tabula library to extract table data from a given PDF document.
        It concatenates all tables found on all pages into a single DataFrame, providing a structured 
        format for the unstructured data in the PDF.
        """
        try:
            # I use tabula to read the tables from a PDF, specifying that I want all pages and tables
            dfs = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
            # I then combine all the tables into one DataFrame for ease of use
            combined_df = pd.concat(dfs, ignore_index=True)
            return combined_df
        except Exception as e:
            print(f"An error occurred while extracting data from PDF: {e}")
            return None

    @staticmethod
    def list_number_of_stores(api_endpoint, headers):
        """
        This method makes an API call to retrieve the number of stores. It's useful for situations 
        where I need to dynamically determine the range of data to process based on the current store 
        count.
        """
        try:
            # I make a GET request to the API and expect a JSON response
            response = requests.get(api_endpoint, headers=headers)
            # I ensure to raise an exception if the call is unsuccessful
            response.raise_for_status()
            # I return the JSON parsed response assuming it directly contains the number of stores  
            return response.json()  
        except requests.RequestException as e:
            print(f"An error occurred while accessing the API: {e}")
            return None

    @staticmethod
    def retrieve_stores_data(store_endpoint, headers, number_of_stores):
        """
        Given an API endpoint, headers for authentication, and a count of stores, this function 
        iterates over each store number to fetch detailed information. It builds a full list of
        stores in a structured DataFrame.
        """
        all_stores = []
        # I loop over the number range of stores to fetch and collate their data
        for store_number in range(1, number_of_stores + 1):
            try:
                # Each store's data is requested individually and added to a list
                response = requests.get(f"{store_endpoint}/{store_number}", headers=headers)
                response.raise_for_status()
                store_data = response.json()
                all_stores.append(store_data)
            except requests.HTTPError as e:
                # I log any errors that aren't server (500) errors 
                if e.response.status_code != 500:
                    print(f"An error occurred while retrieving store {store_number}: {e}")
            except requests.RequestException as e:
                print(f"An error occurred while retrieving store {store_number}: {e}")
        # I convert the list of store data into a DataFrame for further processing
        return pd.DataFrame(all_stores)


    @staticmethod
    def extract_from_s3(s3_address):
        """
        This method extracts data from an S3 bucket given the bucket's address. It's directly reads 
        CSV data directly into a DataFrame without having to download the file first.
        """
        # I parse the S3 address to get the bucket name and object key
        bucket_name = s3_address.split('/')[2]
        object_key = '/'.join(s3_address.split('/')[3:])

        # I initialise a boto3 client to interact with S3
        s3_client = boto3.client('s3')

        # I retrieve the CSV content from S3 and read it directly into a DataFrame
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = response['Body'].read().decode('utf-8')

        # I convert the CSV content to a DataFrame
        return pd.read_csv(StringIO(csv_content))


    @staticmethod
    def extract_json_from_s3(s3_link):
 
        # I parse the S3 address to get the bucket name and object key
        bucket_name = 'data-handling-public' 
        file_key = 'date_details.json'  

        # I initialise a boto3 client to interact with S3
        s3_client = boto3.client('s3')

        # I retrieve the JSON content from S3 and read it directly into a DataFrame
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read()

        # I convert the JSON content to a DataFrame
        json_data = pd.read_json(BytesIO(file_content))
        return json_data