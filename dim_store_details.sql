UPDATE dim_store_details
SET latitude = concat(latitude, lat) 
SET longitude = nullif(longitude, 'N/A')
SET latitude = nullif(latitude, 'N/A')
SET country_code = COALESCE(country_code, 'N/A') 
SET continent = COALESCE(continent, 'N/A') 


SELECT COALESCE(continent, 'N/A') FROM dim_store_details

SELECT concat(lat, latitude) AS latitude FROM dim_store_details

ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE SMALLINT,
	ALTER COLUMN locality TYPE VARCHAR(255),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint,
	ALTER COLUMN opening_date TYPE date,
	ALTER COLUMN store_type TYPE VARCHAR(255),
	ALTER COLUMN latitude TYPE SMALLINT,
	ALTER COLUMN country_code TYPE CHAR(3),
	ALTER COLUMN continent TYPE VARCHAR(255);

ALTER TABLE dim_store_details
DROP COLUMN lat

SELECT MAX(LENGTH(country_code)) AS MAX_LENGTH FROM dim_store_details

SELECT * FROM dim_store_details ORDER BY index ASC