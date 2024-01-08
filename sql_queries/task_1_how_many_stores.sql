-- Task 1: How many stores does the business have and in which countries?

SELECT 
    country_code,
    COUNT (*) AS number_of_stores
FROM dim_store_details 
GROUP BY country_code
ORDER BY
    number_of_stores DESC;