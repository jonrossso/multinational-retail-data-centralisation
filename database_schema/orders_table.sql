/* Casting columns to correct data types
+------------------+--------------------+--------------------+
|   orders_table   | current data type  | required data type |
+------------------+--------------------+--------------------+
| date_uuid        | TEXT               | UUID               |
| user_uuid        | TEXT               | UUID               |
| card_number      | TEXT               | VARCHAR(?)         |
| store_code       | TEXT               | VARCHAR(?)         |
| product_code     | TEXT               | VARCHAR(?)         |
| product_quantity | BIGINT             | SMALLINT           |
+------------------+--------------------+--------------------+
*/

BEGIN;

SELECT * FROM orders_table;

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE orders_table
ALTER COLUMN card_number TYPE VARCHAR(20);

ALTER TABLE orders_table
ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE orders_table
ALTER COLUMN product_code TYPE VARCHAR(12);

ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT;

COMMIT;

-- (Task 9) Referencing the primary keys in the other tables

WITH order_columns AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name = 'orders_table'
    AND table_schema = 'public'
),
dim_columns AS (
    SELECT column_name
    FROM information_schema.columns
    WHERE table_name LIKE 'dim\_%' ESCAPE '\'
    AND table_schema = 'public'
)

SELECT column_name
FROM order_columns
WHERE column_name NOT LIKE 'dim\_%' ESCAPE '\'
  AND column_name NOT IN (SELECT column_name FROM dim_columns); 