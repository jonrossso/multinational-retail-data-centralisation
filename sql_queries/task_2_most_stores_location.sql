-- Task 2: Which locations currently have the most stores?

SELECT
    locality,
    COUNT(*) AS number_of_stores
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY
    number_of_stores DESC
LIMIT 6;