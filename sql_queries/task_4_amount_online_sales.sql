-- Task 4: How many sales are coming from online?

SELECT
    COUNT(orders_table.product_quantity) AS number_of_sales,
    SUM(orders_table.product_quantity) AS product_quantity_count,
    CASE
        WHEN dim_store_details.locality IS NULL THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    orders_table
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    location
ORDER BY
    number_of_sales;