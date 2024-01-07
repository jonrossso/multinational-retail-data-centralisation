/* Updating the dim_store_details table
+---------------------+-------------------+------------------------+
| store_details_table | current data type |   required data type   |
+---------------------+-------------------+------------------------+
| longitude           | TEXT              | FLOAT                  |
| locality            | TEXT              | VARCHAR(255)           |
| store_code          | TEXT              | VARCHAR(?)             |
| staff_numbers       | TEXT              | SMALLINT               |
| opening_date        | TEXT              | DATE                   |
| store_type          | TEXT              | VARCHAR(255) NULLABLE  |
| latitude            | TEXT              | FLOAT                  |
| country_code        | TEXT              | VARCHAR(?)             |
| continent           | TEXT              | VARCHAR(255)           |
+---------------------+-------------------+------------------------+
*/

BEGIN;

SELECT * FROM dim_store_details;

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision;

ALTER TABLE dim_store_details
ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE dim_store_details
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT;

ALTER TABLE dim_store_details
ALTER COLUMN opening_date TYPE DATE USING opening_date::date;

ALTER TABLE dim_store_details
ALTER COLUMN store_type TYPE VARCHAR(255);

ALTER TABLE dim_store_details
ALTER COLUMN store_type DROP NOT NULL;

ALTER TABLE dim_store_details
ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision;

ALTER TABLE dim_store_details
ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details
ALTER COLUMN continent TYPE VARCHAR(255);


-- Task 

/*There are two latitude columns in the store details table. Using SQL, 
merge one of the columns into the other so you have one latitude column.
*/

-- Solution: 

/* I already used data cleaning to remove the lat column which contained 
null values by using the .drop function */

-- Task

/* There is a row that represents the business's website. Change the location 
column values where they're null to N/A. */

-- Solution:

/* I could not find the row which represents the company's website. I even used specific queies to retrieve this
but there were also no rows with null values in the location columns. I also manually inspected.

However, the below will be how I will go about changing the values to "N/A where they are null"

UPDATE dim_store_details
SET locality = 'N/A',
    country_code = 'N/A',
	continent = 'N/A',
WHERE locality IS NULL OR country_code IS NULL OR continent IS NULL;

*/

/* (Task 8) Setting the primary key, but first deleting rows within this column if 
NULL, because a unique identifier is required for relationshiops */


DELETE FROM dim_store_details WHERE store_code IS NULL;

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

COMMIT;

-- (Task 9) Setting the foreign key

/* I did this to sync store_code values from orders_table to dim_store_details, 
to ensure that dim_store_details contains all the latest store_code 's */

INSERT INTO dim_store_details (store_code)
SELECT DISTINCT store_code
FROM orders_table
WHERE store_code IS NOT NULL
  AND store_code NOT IN (SELECT store_code FROM dim_store_details);

/* I added a foreign key constraint named "fk_orders_table_store_code" which references the 
store_code in dim_store_details. I also used "ON DELETE" and "ON UPDATE" to create a relational
link which handles the cases where store_code 's are deleted or updated in dim_store_details */

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code)
ON DELETE SET NULL
ON UPDATE CASCADE;