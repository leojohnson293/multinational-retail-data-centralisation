ALTER TABLE order_table
	ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid,
	ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid,
	ALTER COLUMN card_number TYPE VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN product_quantity TYPE SMALLINT;
	
SELECT * FROM order_table

SELECT MAX(LENGTH(card_number)) AS MAX_LENGTH FROM order_table