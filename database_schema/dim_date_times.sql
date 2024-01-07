-- (Task 6) Updating the dim_date_times table

-- Changing dim_date_times column data types

/*
+-----------------+-------------------+--------------------+
| dim_date_times  | current data type | required data type |
+-----------------+-------------------+--------------------+
| month           | TEXT              | VARCHAR(?)         |
| year            | TEXT              | VARCHAR(?)         |
| day             | TEXT              | VARCHAR(?)         |
| time_period     | TEXT              | VARCHAR(?)         |
| date_uuid       | TEXT              | UUID               |
+-----------------+-------------------+--------------------+

*/
BEGIN;

SELECT * FROM dim_date_times;

ALTER TABLE dim_date_times
ALTER COLUMN timestamp TYPE VARCHAR(20);
ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(3);
ALTER TABLE dim_date_times
ALTER COLUMN year TYPE VARCHAR(6);
ALTER TABLE dim_date_times
ALTER COLUMN day TYPE VARCHAR(2);
ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10);
ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

/* (Task 8) Setting the primary key, but first deleting rows within this column if 
NULL, because a unique identifier is required for relationshiops */

DELETE FROM dim_date_times WHERE date_uuid IS NULL;

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

COMMIT;

-- (Task 9) Setting the foreign key 

/* I did this to sync date_uuid values from orders_table to dim_date_times, 
to ensure that dim_date_times contains all the latest date_uuid s */

INSERT INTO dim_date_times (date_uuid)
SELECT DISTINCT date_uuid
FROM orders_table
WHERE date_uuid IS NOT NULL
  AND date_uuid NOT IN (SELECT date_uuid FROM dim_date_times);

/* I added a foreign key constraint named "fk_orders_table_date_uuid" which references the 
date_uuid in dim_date_times. I also used "ON DELETE" and "ON UPDATE" to create a relational
link which handles the cases where date_uuid 's are deleted or updated in dim_date_times */

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_date_uuid
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid)
ON DELETE SET NULL
ON UPDATE CASCADE;