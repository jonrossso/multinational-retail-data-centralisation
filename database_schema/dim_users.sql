/* Casting columns to correct data types
+----------------+--------------------+--------------------+
| dim_users | current data type  | required data type |
+----------------+--------------------+--------------------+
| first_name     | TEXT               | VARCHAR(255)       |
| last_name      | TEXT               | VARCHAR(255)       |
| date_of_birth  | TEXT               | DATE               |
| country_code   | TEXT               | VARCHAR(?)         |
| user_uuid      | TEXT               | UUID               |
| join_date      | TEXT               | DATE               |
*/

BEGIN;

SELECT * FROM dim_users;

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255);
ALTER TABLE dim_users
ALTER COLUMN last_name TYPE VARCHAR(255);
ALTER TABLE dim_users
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date;
ALTER TABLE dim_users
ALTER COLUMN country_code TYPE VARCHAR(3);
ALTER TABLE dim_users
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;
ALTER TABLE dim_users
ALTER COLUMN join_date TYPE DATE USING join_date::date;

/* (Task 8) Setting the primary key, but first deleting rows within this column if 
NULL, because a unique identifier is required for relationshiops */

DELETE FROM dim_users WHERE user_uuid IS NULL;

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

COMMIT;

-- (Task 9) Setting the foreign key

/* I did this to sync user_uuid values from orders_table to dim_users, 
to ensure that user_uuid s contains all the latest user_uuid s */

INSERT INTO dim_users (user_uuid)
SELECT DISTINCT user_uuid
FROM orders_table
WHERE user_uuid IS NOT NULL
  AND user_uuid NOT IN (SELECT user_uuid FROM dim_users); 

/* I added a foreign key constraint named "fk_orders_table_user_uuid" which references the 
user_uuid in dim_users. I also used "ON DELETE" and "ON UPDATE" to create a relational
link which handles the cases where user_uuid 's are deleted or updated in dim_users */

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_user_uuid
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid)
ON DELETE SET NULL
ON UPDATE CASCADE;