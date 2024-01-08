-- Task 5: What percentage of sales come through each type of store?

WITH total_sales AS (
    SELECT 
        SUM(orders_table.product_quantity * CAST(REPLACE(dim_products.product_price, '£', '') AS NUMERIC)) AS total
    FROM 
        orders_table
    JOIN 
        dim_products ON orders_table.product_code = dim_products.product_code
)

SELECT
    dim_store_details.store_type, 
    SUM(orders_table.product_quantity * CAST(REPLACE(dim_products.product_price, '£', '') AS NUMERIC)) AS revenue,
    (SUM(orders_table.product_quantity * CAST(REPLACE(dim_products.product_price, '£', '') AS NUMERIC)) / total_sales.total) * 100.0 AS percentage_total
FROM 
    orders_table
JOIN 
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code, total_sales
GROUP BY 
    dim_store_details.store_type, total_sales.total
ORDER BY 
    percentage_total DESC;