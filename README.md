# Multinational Retail Data Centralisation Project by Jon-Ross

## Table of Contents
- **[Project Description](#project-description)**
- **[Learning Outcomes](#learning-outcomes)**
- **[Installation](#installation)**
- **[Usage](#usage)**
- **[File Structure](#file-structure)**
- **[License](#license)**

## Project Description

This is my personal journey through building a data centralisation pipeline for a multinational retail company. The main functions so far are:

1. Extracting data from various external data sources such as CSV files, S3 buckets, and APIs.  
2. Finding inconsistencies within the extracted data and cleaning them by implementing various cleaning methods. 
3. Centralising the cleaned data into my local PostgreSQL database using pgAdmin 4, creating a singular, well-organised database that is ready for analysis.

### Learning Outcomes

Throughout this project, I have perosnally learnt how to find data with inconsistencies and clean datadataframes. I have also learnt about how to deal with the security of credntials for example, by placing them in a .gitignore file to prevent it from uploading to a remote repository. I have understood even clearer the importance of data integrity and generally using Python for automating tasks.

### Highlights of My Work:
- **Credential Security**: I segregated local postgreSQL and remote credentials into separate files and including them in `.gitignore`, I minimised security risks. Also, I made use of private secure methods underscored for database connection.
- **Streamlined Workflow**: My main.py script clearly brings together all components of the pipeline, managing the flow of data from extraction to storage.
- **Thorough Data Cleaning**: eg. I removed underscores and replaced dashes with spaces within categorical data to achieve consistency.
- **Masking Sensitive Data**: I took extra care to mask sensitive card numbers, ensuring data security.
- **Data Standardisation**: eg. I uniformly capitalised product codes and corrected typos like changing "Still_avaliable" to "Still available".
- **Column Standardisation**: eg. I renamed unnamed columns to 'index', aligning with other tables' structures.

## Installation
To set up this project:

1. **Clone the Repo**: Download the project to your local system.
2. **Python Setup**: Ensure Python 3.8+ is installed.
3. **Credential Files**: Place the credential files `db_creds.yaml` and `local_creds.yaml` in the project root.

## Usage
Run the data pipeline:

1. **Open Terminal**: Go to the project directory.
2. **Execute Script**: Run `python3 main.py` to initiate the data processing.
3. **Automatic Processing**: The scripts will extract, clean, and upload the data autonomously.

## File Structure
- **`data_extraction.py`**: Manages the retrieval of data from various sources including RDS, S3, and APIs.
Some sample methods
  - `read_rds_table()`: Connects to the database and pulls the specified table into a DataFrame.
  - `retrieve_pdf_data()`: Downloads and extracts tables from PDF documents.

- **`data_cleaning.py`**: Where I meticulously cleanse the data ensuring its quality and consistency.
Some sample methods
  - `_clean_user_data()`: Cleans user data, dealing with nulls and standardising dates.
  - `_convert_product_weights()`: Converts product weights into a consistent unit.

- **`database_utils.py`**: Facilitates database interactions, from connecting to executing data transactions.
Some sample methods
  - `_init_db_engine()`: Initialises the database connection engine.
  - `_upload_to_local_db()`: Uploads DataFrames to the specified local database tables.

- **`main.py`**: The orchestrator that uses the above components to run the data extraction, cleansing, and loading.

## License
This project adheres to the MIT License.

