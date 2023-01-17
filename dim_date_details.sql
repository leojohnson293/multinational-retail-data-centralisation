ALTER TABLE dim_date_details
	ALTER COLUMN month TYPE CHAR(2),
	ALTER COLUMN year TYPE CHAR(4),
	ALTER COLUMN day TYPE CHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(10),
	ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
	-- ALTER COLUMN timestamp TYPE TIMESTAMP USING time::time;
SELECT * FROM dim_date_details 

SELECT MAX(LENGTH(time_period)) AS MAX_LENGTH FROM dim_date_details