# multinational-retail-data-centralisation

## Business Questions

 > How many stores does the business have and in which countries?

The first line of the SQL query from below gives you the total number of stores which is 441. The rest of the query shows how many of those stores are in each country. GB has the most amount of stores with 265, then it is Germany with 141 and the US has 34 stores. There is also one store that is not based in a country due to it being online.
```SQL
SELECT count(store_code) FROM dim_store_details

SELECT country_code, COUNT(store_code) FROM dim_store_details 
GROUP BY country_code 
```
> Which locations currently have the most stores?

From using the following query, it can be deduced that the location with the most stores is Chapletown which has 14 stores.
```SQL
SELECT locality, COUNT(store_code) AS total_no_stores FROM dim_store_details 
GROUP BY locality ORDER BY COUNT(store_code) DESC
```
> Which months produce the most sales typically?

Using the following query, the highest number of sales are produced in July and the lowest is produced in February.
 ```SQL
SELECT count(*) AS total_sales, month FROM dim_date_details 
GROUP BY dim_date_details.month ORDER BY count(*) DESC
```
> How many sales are coming from online?

The query below shows that 107739 sales are coming from online.
 ```SQL
SELECT COUNT(*)AS number_of_sales, 
SUM(product_quantity) as product_quantity_count,
CASE store_code
WHEN 'WEB-1388012W' THEN 'Web'
ELSE 'Offline'
END AS location
FROM order_table
GROUP BY location
 ```
> What percentage of sales come through each type of store?
The following query provides the percentages of sales for each store type, with the local store type having the highest. 
 ```SQL
SELECT dim_store_details.store_type, COUNT(*) AS total_sales,
count(*) * 100 / sum(count(*)) over () AS "percentage_total(%)"
FROM order_table
INNER JOIN dim_store_details
ON dim_store_details.store_code = order_table.store_code
GROUP BY dim_store_details.store_type
 ```
> Which month in each year produced the most sales?
 ```SQL

 ```
> What is our staff headcount?

 ```SQL
SELECT SUM(staff_numbers) AS total_staff_numbers,
CASE country_code WHEN 'N/A' Then 'WEB' ELSE country_code  END AS country_code
FROM dim_store_details
GROUP BY country_code ORDER BY SUM(staff_numbers) DESC
 ```
> Which German store type is selling the most?
 ```SQL

 ```
> How quickly is the company making sales?
 ```SQL

 ```