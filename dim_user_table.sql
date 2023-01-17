ALTER TABLE dim_users
	ALTER COLUMN first_name TYPE VARCHAR(255),
	ALTER COLUMN last_name TYPE VARCHAR(255),
	ALTER COLUMN date_of_birth TYPE date USING join_date::date,
	ALTER COLUMN country_code TYPE CHAR(2),
	ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid, 
	ALTER COLUMN join_date TYPE date USING join_date::date;
SELECT * FROM dim_users


SELECT * FROM dim_users