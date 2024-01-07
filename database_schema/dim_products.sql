-- (Task 4) Updating the dim_products table 

BEGIN;

SELECT * FROM dim_products;

-- Removing the beginning "£" from the product_price column

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '')
WHERE product_price LIKE '£%';

-- Creating the weight_class column

SELECT * FROM dim_products;
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(15);

-- This is the weight classification chart:


/*
+--------------------------+-------------------+
| weight_class VARCHAR(?)  | weight range(kg)  |
+--------------------------+-------------------+
| Light                    | < 2               |
| Mid_Sized                | >= 2 - < 40       |
| Heavy                    | >= 40 - < 140     |
| Truck_Required           | => 140            |
+----------------------------+-----------------+
*/

ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
    WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
    WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
    WHEN weight >= 140 THEN 'Truck_Required'
    ELSE 'Unknown'
END;

/* (Task 5) Changing a dim_products column name from removed to still_available and then changing
individual data types */

-- Changing column name

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;


-- Changing data types

ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT;
ALTER TABLE dim_products
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;
ALTER TABLE dim_products
ALTER COLUMN "EAN" TYPE VARCHAR(17);
ALTER TABLE dim_products
ALTER COLUMN product_code TYPE VARCHAR(12);
ALTER TABLE dim_products
ALTER COLUMN date_added TYPE DATE USING date_added::date;
ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::uuid;
ALTER TABLE dim_products
ALTER COLUMN still_available TYPE BOOLEAN USING CASE WHEN still_available = 'true' THEN TRUE ELSE FALSE END;
ALTER TABLE dim_products
ALTER COLUMN weight_class TYPE VARCHAR(15);



/* (Task 8) Setting the primary key, but first deleting rows within this column if 
NULL, because a unique identifier is required for relationshiops */

DELETE FROM dim_products WHERE product_code IS NULL;

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

COMMIT;

-- (Task 9) Setting the foreign key

/* I did this to sync product_code values from orders_table to dim_products, 
to ensure that dim_products contains all the latest product codes */

INSERT INTO dim_products (product_code)
SELECT DISTINCT product_code
FROM orders_table
WHERE product_code IS NOT NULL
  AND product_code NOT IN (SELECT product_code FROM dim_products);

/* I added a foreign key constraint named "fk_orders_table_product_code" which references the 
product_code in dim_products. I also used "ON DELETE" and "ON UPDATE" to create a relational
link which handles the cases where product codes are deleted or updated in dim_products */

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_table_product_code
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code)
ON DELETE SET NULL
ON UPDATE CASCADE;