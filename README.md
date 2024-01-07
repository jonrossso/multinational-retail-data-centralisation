# Multinational Retail Data Centralisation Project by Jon-Ross

## Table of Contents
- **[Project Description](#project-description)**
- **[Learning Outcomes](#learning-outcomes)**
- **[Progression Hurdles](#progression-hurdles)**
- **[Installation](#installation)**
- **[Usage](#usage)**
- **[File Structure](#file-structure)**
- **[License](#license)**

## Project Description

This is my personal journey through building a Data Centralisation pipeline for a Multinational Retail Company. Here is a list of the main things I have managed to accomplish:

1. Extracting data from various external data sources such as CSV files, S3 buckets, and APIs.  
2. Finding inconsistencies within extracted data and cleaning them by using various cleaning methods both within Python and SQL code.
3. Centralising the cleaned data into my local PostgreSQL database using pgAdmin 4, creating a  well-organised local/central database.
4. Creating a database schema where I inspected data and used SQL to convert respective columns into correct designated data types as well as make various changes to tables.
5. Identifying and solving conflicting errors before adding  primary and foreign keys to complete the database schema.
6. Querying and extracting data using SQL for insight analysis.

### Learning Outcomes

Throughout this project, I have personally learnt how to find data with inconsistencies and clean pandas dataframes. I have learnt how to work around and intepret errors. I understand the importance of database security and how to deal with the security of credentials for example, by placing all credentials in a .gitignore files to prevent it from uploading to a remote repository. I have understood even clearer the importance of data integrity and generally using Python for automating tasks. I have learnt how to develop a database schema and develop primary and foreign key relationships. Importantly, the entire concept of data centralisation, where data from various sources is extracted and loaded into a destination for the purpose of analysis and to make informed decisions is clear to me.

### Progression Hurdles 

As I progressed to each stage, I noticed that I had more data cleaning to do, so I had to reccurently go back to data_cleaning.py. I enjoyed the entire process of making sure my data was cleaned efficiently. During the creation of my database schema, I kept on obtaining the error: "insert or update on table "xxxxx" violates foreign key constraint. The reason I kept obtaining this error was because there were some rows that existed within the orders_table that were not present within the other tables. To solve this, I used SQL queries to find out which rows they were. I finally resolved this issue by using "ON UPDATE" within my SQL queries when adding foreign key constraints to create relational links which handled updates in respective tables.


### Highlights of My Work:
- **Credential Security**: I segregated local postgreSQL and remote credentials into separate files and including them in `.gitignore`, I minimised security risks. Also, I made use of private secure methods underscored for database connection.
- **Streamlined Workflow**: My main.py script clearly brings together all components of the pipeline, managing the flow of data from extraction to storage.
- **Thorough Data Cleaning**: eg. I removed underscores and replaced dashes with spaces within categorical data to achieve consistency.
- **Masking Sensitive Data**: I started off by masking sensitive card_numbers, but I realised that I needed the card_numbers to develop my schema (since it is a primary key) so I had comment this function within my data_cleaning.py.
- **Data Standardisation**: For example, I uniformly capitalised product codes and corrected typos like changing "Still_avaliable" to "Still available".
- **Column Standardisation**: For example, I renamed "Unnamed" columns to 'index', aligning with other tables' structures.

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

- **`main.py`**: The facilitator that uses the above components to run the data extraction, cleansing, and loading.

- **`requirements.txt`**: Necessary Python packages and their versions required for the project.

- **`YAML Files (*.yaml)`**: Contains credentials.

- **`README.md`**: Document explaining the purpose, structure, and usage of this project.

- **`.gitignore`**: Intentionally untracked files to ignore

- **`.sql Scripts (*.sql)`**: SQL queries and commands for database manipulation

## License
This project adheres to the MIT License.