import yaml
from sqlalchemy import create_engine, MetaData, exc
from sqlalchemy.exc import SQLAlchemyError

class DatabaseConnector:
    """DatabaseConnector class is a tool for managing both local PostgresSQL and AWS RDS database connections and interactions.
    """
    # This private method reads database credentials from a YAML file for AWS RDS.
    def _read_db_creds(self):
        try:
            with open('db_creds.yaml', 'r') as file:
                creds = yaml.safe_load(file)  # Safe loading
            return creds
        except Exception as e:
            print(f"Error reading database credentials: {e}")
            return None

   # Similar to the AWS RDS credentials, this method reads local database credentials.
    def _read_local_db_creds(self):
        """
        Reads local database credentials from a YAML file and returns a dictionary
        containing the credentials.
        """
        try:
            with open('local_creds.yaml', 'r') as file:
                creds = yaml.safe_load(file)
            return creds
        except Exception as e:
            print(f"Error reading local database credentials: {e}")
            return None
    
    # This private method initialises a connection to the AWS RDS database and uses credentials to read from
    # the YAML file and creates a SQLAlchemy engine.
    def _init_db_engine(self):
        try:
            creds = self._read_db_creds()
            if creds is None:
                raise ValueError("Database credentials are not available.")

            db_conn_url = create_engine(
                f"postgresql+psycopg2://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
            )
            return db_conn_url
        except SQLAlchemyError as e:
            print(f"Error initialising database engine: {e}")
            return None
        
    # Lists all the tables in the connected database using SQLAlchemy's MetaData.
    # This method is useful for getting an overview of the database schema.
    def _list_db_tables(self):
        """
        List all tables in the connected database.
        
        :return: A list of table names.
        """
        try:
            engine = self._init_db_engine()
            if engine is None:
                raise ValueError("Database engine is not initialised.")
            
            # Creating an instance of MetaData
            metadata = MetaData()
            
            # Reflecting the tables from the engine (i.e., the database)
            metadata.reflect(engine)

            # Returning the list of table names
            return list(metadata.tables.keys())
        except exc.SQLAlchemyError as e:
            print(f"Error listing database tables: {e}")
            return None
    

    # I use this method to upload data to the local 'sales_data' database.
    # It is a crucial part of the data pipeline, allowing for local storage of processed data.
    def _upload_to_local_db(self, dataframe, table_name, if_exists='append', index=False):
        try:
            local_engine = self.init_local_db_engine()
            if local_engine is None:
                raise ValueError("Local database engine is not initialised.")
            
            dataframe.to_sql(name=table_name, con=local_engine, if_exists=if_exists, index=index)
            print(f"Data successfully uploaded to local table {table_name}.")
        except SQLAlchemyError as e:
            print(f"Error uploading data to the local database: {e}")


    # Initialising a connection to the local PostgreSQL database.
    def init_local_db_engine(self):
        """
        Initialises an SQLAlchemy engine for the local PostgreSQL database.
        """
        try:
            creds = self._read_local_db_creds()
            if creds is None:
                raise ValueError("Local database credentials are not available.")

            local_db_conn_url = f"postgresql+psycopg2://{creds['LOCAL_USER']}:{creds['LOCAL_PASSWORD']}@{creds['LOCAL_HOST']}:{creds['LOCAL_PORT']}/{creds['LOCAL_DATABASE']}"
            local_engine = create_engine(local_db_conn_url) # Allows connection and operation on the database.
            return local_engine
        except SQLAlchemyError as e:
            print(f"Error initialising local database engine: {e}")
            return None



