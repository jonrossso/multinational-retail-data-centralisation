-- (Task 7) Updating the dim_card_details table

/* Cast to types
+------------------------+-------------------+--------------------+
|    dim_card_details    | current data type | required data type |
+------------------------+-------------------+--------------------+
| card_number            | TEXT              | VARCHAR(?)         |
| expiry_date            | TEXT              | VARCHAR(?)         |
| date_payment_confirmed | TEXT              | DATE               |
+------------------------+-------------------+--------------------+
*/

BEGIN;

SELECT * FROM dim_card_details;

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(20);

ALTER TABLE dim_card_details
ALTER COLUMN expiry_date TYPE VARCHAR(22);

ALTER TABLE dim_card_details
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

/* (Task 8) Setting the primary key, but first deleting rows within this column if 
NULL, because a unique identifier is required for relationshiops */

DELETE FROM dim_card_details WHERE card_number IS NULL;

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

COMMIT;

--(Task 9) Setting the foreign key 

/* I did this to sync card_number values from orders_table to dim_card_details, 
to ensure that dim_card_details contains all the latest card_number s */

INSERT INTO dim_card_details (card_number)
SELECT DISTINCT card_number
FROM orders_table
WHERE card_number IS NOT NULL
  AND card_number NOT IN (SELECT card_number FROM dim_card_details);

/* I added a foreign key constraint named "fk_orders_table_card_number" which references the 
card_number in dim_card_details. I also used "ON DELETE" and "ON UPDATE" to create a relational
link which handles the cases where card_number 's are deleted or updated in dim_card_details */

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_card_number
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number)
ON DELETE SET NULL
ON UPDATE CASCADE;