ALTER TABLE dim_products
ADD COLUMN weight_class varchar(20)
RENAME COLUMN removed TO still_available
RENAME COLUMN product_price TO "product_price(£)"

UPDATE dim_products
SET weight_class = 
                    CASE WHEN ("Weight(kg)" > 140) THEN 'Truck_Required' 
                     WHEN ("Weight(kg)" BETWEEN 40 AND 140)  THEN 'Heavy' 
                     WHEN ("Weight(kg)" BETWEEN 2 AND 40)  THEN 'Mid_Sized' 
                     WHEN ("Weight(kg)" < 2) THEN 'Light' END

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '') 

ALTER TABLE dim_products
	ALTER COLUMN "product_price(£)" TYPE float4 USING "product_price(£)"::real,
	ALTER COLUMN "Weight(kg)" TYPE float4 USING "Weight(kg)"::real,
	ALTER COLUMN "EAN" TYPE VARCHAR(17),
	ALTER COLUMN date_added TYPE date,
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
	ALTER COLUMN weight_class TYPE VARCHAR(14);

SELECT MAX(LENGTH(weight_class)) AS MAX_LENGTH FROM dim_products
SELECT * FROM dim_products ORDER BY index ASC
