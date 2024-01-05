from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

"""
This is the main entry point for the data processing pipeline. It actuates the data extraction, cleaning, and loading 
processes by utilising the DatabaseConnector, DataExtractor, and DataCleaning classes.
"""

def main():
    try:
        # I initialise instances of the utility classes to be used for various database operations and data handling.
        db_connector = DatabaseConnector()
        data_extractor = DataExtractor()
        data_cleaner = DataCleaning()

        # I specify the names of the RDS and local tables from which I extract and to which I load the data.
        rds_table_name = 'legacy_users'
        local_table_name = 'dim_card_details'  # New local table for card details

        # Extracting data from the remote RDS table and handle any failures in the process.
        rds_dataframe = data_extractor.read_rds_table(db_connector, rds_table_name)
        if rds_dataframe is None:
            raise ValueError(f"Failed to extract data from table: {rds_table_name}")

        # Cleaning the extracted data using the custom cleaning method for user data.
        cleaned_rds_dataframe = data_cleaner._clean_user_data(rds_dataframe)

        # Uloading the cleaned data into the designated local database table and confirm the success of the operation.
        db_connector._upload_to_local_db(cleaned_rds_dataframe, table_name='dim_users')
        print("RDS data extraction, cleaning, and upload completed successfully.")

        # Retrieving data from a PDF file stored in an S3 bucket using a predefined link.
        pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        pdf_dataframe = DataExtractor.retrieve_pdf_data(pdf_link)
        if pdf_dataframe is None:
            raise ValueError("Failed to extract data from PDF.")
        
        if pdf_dataframe is None:
            raise ValueError("Failed to extract data from PDF.")

        # Cleaning the extracted PDF data, standardising and sanitising the information as needed.
        cleaned_pdf_dataframe = DataCleaning._clean_card_data(pdf_dataframe)

        # Uploading the cleaned PDF data to the 'dim_card_details' table in the local 'sales_data' database
        db_connector._upload_to_local_db(cleaned_pdf_dataframe, table_name='dim_card_details')
        print("PDF data extraction, cleaning, and upload completed successfully.")

        # I define the headers and endpoint for accessing the API that holds the number of stores.
        headers = {
            'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        }
        number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'


        # Retrieving the count of stores from the API and validate the response.
        number_of_stores_data = DataExtractor.list_number_of_stores(number_of_stores_endpoint, headers)
        if number_of_stores_data is None:
            raise ValueError("Failed to retrieve the number of stores.")

        # Parsing the response to extract the actual number of stores.
        number_of_stores = number_of_stores_data.get('number_stores')
        if number_of_stores is None:
            raise ValueError("Number of stores is not available in the API response.")


        #  Retrieving detailed data for each store using the store count and API endpoint.
        store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details'
        headers = {
            'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
        }

        # Cleaning the retrieved store data
        stores_data = DataExtractor.retrieve_stores_data(store_endpoint, headers, number_of_stores)
        if stores_data is None or stores_data.empty:
            raise ValueError("Failed to retrieve stores data.")

        # Cleaning the stores data
        cleaned_stores_data = DataCleaning.clean_store_data(stores_data)

        # Uploading the cleaned data to the 'dim_store_details' table in the local 'sales_data' database

        db_connector._upload_to_local_db(cleaned_stores_data, table_name='dim_store_details')
        print("Store data extraction, cleaning, and upload completed successfully.")
        # Extracting product data from an S3 bucket and converting the weights to a standard unit of kilograms.
        s3_address = 's3://data-handling-public/products.csv'
        products_df = DataExtractor.extract_from_s3(s3_address)
        # Converting product weights to kilograms
        products_df = DataCleaning._convert_product_weights(products_df)
        # Cleaning the products data
        products_df = DataCleaning.clean_products_data(products_df)
        # Uploading the cleaned data to the local database
        db_connector._upload_to_local_db(products_df, table_name='dim_products')
        print("Product data extraction, cleaning, and upload completed successfully.")

        db_connector = DatabaseConnector()


        # I list the database tables to find the one with order information and then extract the relevant data.
        table_names = db_connector._list_db_tables()
        orders_table_name = next((name for name in table_names if "order" in name.lower()), None)
        if orders_table_name is None:
            print("Orders table not found")
        else:
            print(f"Orders table found: {orders_table_name}")


        # I extract order data from the RDS, clean it, and upload it to the local database.
        if orders_table_name:
            orders_df = DataExtractor.read_rds_table(db_connector, orders_table_name)
            if orders_df.empty:
                print("No data found in orders table")
            else:
                print("Orders data extracted successfully")

        # Uploading the cleaned orders data to the database under the orders_table
        if orders_table_name and not orders_df.empty:
            cleaned_orders_df = DataCleaning.clean_orders_data(orders_df)
            db_connector._upload_to_local_db(cleaned_orders_df, 'orders_table', if_exists='replace')  # Assuming you want to replace the existing table
            print("Cleaned orders data uploaded successfully")




        # Extracting the JSON data from S3
        date_events_df = DataExtractor.extract_json_from_s3('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
        
        # Cleaning the extracted data
        cleaned_date_events_df = DataCleaning.clean_date_events_data(date_events_df)

        # Uploading the cleaned data to the 'dim_date_times' table in the local 'sales_data' database
        db_connector = DatabaseConnector()
        db_connector._upload_to_local_db(cleaned_date_events_df, 'dim_date_times', if_exists='replace')

        print("Date events data extraction, cleaning, and upload completed successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


# I make sure that the script can run directly when executed, rather than being imported as a module.
if __name__ == '__main__':
    main()