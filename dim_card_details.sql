ALTER TABLE dim_card_details
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN expiry_date TYPE VARCHAR(5),
	ALTER COLUMN date_payment_confirmed TYPE date;
SELECT * FROM dim_card_details
SELECT MAX(LENGTH(expiry_date)) AS MAX_LENGTH FROM dim_card_details