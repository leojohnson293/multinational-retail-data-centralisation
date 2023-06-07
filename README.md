# ***Multinational Retail Data Centralisation***
> This project involes being part of a multinational company whose sales data is spread across different sources. Here, the data will be extracted from those sources, cleaned using the Pandas library and sent to a PostgreSQL database to be queried.

## ***Project Summary***
The extraction of the tables of data from different sources which is then cleaned and sent to a database follows the ETL(Extract, Transform and Load) principle used in data pipelines. This process will take place using three different scripts which are in the repository. They are the database_utils.py, data_extraction.py and data_cleaning.py files. The database_utils.py file will connect to an AWS RDS database to extract some of the tables in the data_extraction.py file and send the cleaned data from the data_cleaning.py file to the SQL database. The data_extraction.py file will be extracting the data from the various sources and will be send them to the data_cleaning.py file to be cleaned and ready to be sent to PostgreSQL. When all the cleaned data are in SQL, the data types for each column will be changed to the correct ones so they can be queried to answer business questions.

There will be six tables that will be extracted and cleaned in this project:
- The orders table.
- The users table.
- The store details table.
- The product table.
- The card details table.
- The date table.

## ***Data Extraction***
>This part of the project will be extracting the data from different sources which was done on the data_extraction.py file.
### _Extracting from AWS RDS_
Firstly, the data is extracted from an AWS RDS database using SQLAlchemy in the database_utils.py file. The code below shows the `DatabaseConnector` class from the database_utils.py file, which shows the `init_db_engine` method which initialises the engine for the RDS, using crediantials stored in a yaml file that is accessed in the `__init__` method of the class. The `list_db_tables` method is used to list all of the tables that are in the RDS database to know which tables can be extracted from it. 

```python
from sqlalchemy import create_engine as ce
from sqlalchemy import inspect

class DatabaseConnector:
    def __init__(self):
        self.yaml = r'C:\Users\acer laptop\Desktop\AiCore\multinational-retail-data-centralisation\db_creds.yaml'
        with open(self.yaml) as f:
            self.read_db_creds = yaml.safe_load(f)
        
    
    def init_db_engine(self):
        self.db_type ='postgresql'
        self.db_api ='psycopg2'
        self.db_host = self.read_db_creds['RDS_HOST']
        self.db_password = self.read_db_creds['RDS_PASSWORD']
        self.db_user = self.read_db_creds['RDS_USER']
        self.db_database = self.read_db_creds['RDS_DATABASE']
        self.db_port = self.read_db_creds['RDS_PORT']
        self.engine = ce(f"{self.db_type}+{self.db_api}://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_database}")
        return self.engine

    def list_db_tables(self):
        self.inspector = inspect(self.engine)
        print(self.inspector.get_table_names())
```
Then in the data_extraction.py file, the  `DataExtractor` class was created and the table was read on the `read_RDS_data` method and extracted on the `extract_RDS_table` method, both using SQLAlchemy.

```python
class DataExtractor: 
    def __init__(self):
        db_con = DatabaseConnector()
        self.engine = db_con.init_db_engine()
        pd.set_option('display.max_rows', None)
        self.pdf ='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        keys = r'C:\Users\acer laptop\Desktop\AiCore\leo_accessKeys.yaml'
        with open(keys) as f:
            self.aws_keys = yaml.safe_load(f)

    def read_RDS_data(self):
        self.orders_table = self.engine.execute('''SELECT * FROM orders_table''').fetchall()
        return self.orders_table

    def extract_RDS_table(self):
        self.df_orders_table =pd.read_sql_table('orders_table', self.engine)
        print(self.df_orders_table.head())
        return self.df_orders_table
```
### _Extract from PDF_
The next set of data that needed to be extracted was the card details of the payments for each order inside a PDF file  which is initialized in the `__init__` method as `self.pdf` in the code above. This extraction will be done using the tabula python library. As seen in the code below, tabula reads the pdf and uses `convert_into` function to take the pdf and converts it to a csv file called 'output.csv'. Then that csv file is edited and cleaned up to ensure it includes all rows of the pdf file, as some are split between pages and would create a formatting error when trying to read it using Pandas. Once fully cleaned, the csv file is renamed to 'new_output.csv' and is read by Pandas using the `read_csv` function as seen in the code below.
```python
import tabula

    def retrieve_pdf_data(self):
        self.card_details=tabula.read_pdf(self.pdf, lattice=True, pages= 'all', stream=True)
        tabula.convert_into(self.pdf, "output.csv", output_format="csv", pages='all')
        self.pd_card_details = pd.read_csv('new_output.csv') 
        return self.pd_card_details
```
### _Extract from RestfulAPI_
After that, the data for all the stores had to be extracted from an API. Here, `requests.get` is used to access an API endpoint using the headers which are the keys to it and is initialized when running the code in the `__init__` method as `self.store_header` (but aren't included here due to security reasons). Here two methods are created. The first method `list_number_of_stores` returns the number of stores in the business. This will then be used in the second method which is the `retrieve_stores_data` method, to run a for-loop through the second endpoint to get the data for each store in a dictionary format. Each dictionary will be appended to an empty list and made into a Pandas DataFrame.
```python
    def list_number_of_stores(self):
        number_of_stores = requests.get('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', headers = self.store_header)
        return number_of_stores.json()['number_stores']
    
    def retrieve_stores_data(self):
        all_stores = []
        for store_number in range(self.list_number_of_stores()):  
            self.stores = requests.get(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}', headers = self.store_header)
            self.stores = (self.stores.json())
            all_stores.append(self.stores)
        self.df_stores = pd.DataFrame(all_stores, index = range(0, (len(all_stores))))
        #print(self.df_stores)
        return self.df_stores
```

### _Extract from AWS S3_
The final location where the data needs to extracted is from an AWS S3 bucket. The code below shows the csv file of the products table extracted from an S3 bucket. In this scenario, the boto3 library will be used to access it. First, the S3 client is created using `boto3.client`, the access key ID and secret access key for the IAM role for access from a dictionary initialized in the `__init__` method called `self.aws_keys`, were given as arguements. The csv file from the bucket is then decoded and used to create a Pandas DataFrame as seen in th ecode below.
```python
    def extract_from_s3(self):
        client = boto3.client('s3',aws_access_key_id= self.aws_keys['Access key ID'], aws_secret_access_key=self.aws_keys['Secret access key'])
        csv_object = client.get_object(Bucket = 'data-handling-public', Key = 'products.csv' )['Body']
        csv_object = csv_object.read().decode('utf-8')
        from io import StringIO
        df_product_details = pd.read_csv(StringIO(csv_object))
        return df_product_details
```

---
## ***Data Cleaning***
> After extracting each table from the various data soruces, the tables had to be cleaned using the Pandas library in the data_cleaning.py file.

In the data_cleaning.py file, the `DataExtractor` class is imported from the data_extraction.py file and the `DataClean` class is created. In the `__init__` method of the `DataClean` class, each of the tables that were extracted in the `DataExtractor` class are initialised as attributes as seen below.

```python
class DataClean:
    def __init__(self):
        db_ext = DataExtractor()
        self.df_rds = db_ext.extract_RDS_table() # gets the users and orders table
        self.df_cd = db_ext.retrieve_pdf_data() # gets the card details table
        self.df_lsd = db_ext.retrieve_stores_data() # gets the store details table
        self.df_prod = db_ext.extract_from_s3() # gets the product table
        self.df_date = db_ext.extract_json_data() # gets the date table
```
Then in the subsequent methods of the `DataClean` class, the tables were cleaned using the Pandas library. Below is the `clean_store_data` method, which is used to clean the store details table. In the method `str.replace` is used to remove any unwanted values inside a columns which in this case is used for the 'continent' and 'staff_numbers' columns.  Then the Pandas function `pd.to_datetime`is used to ensure all values in the 'opening_date' column are in a date format. For example, a variable in that column may appear as a string such as '20 October 2015', so `pd.to_datetime` will convert it to '2015/10/20', so it can be used in date-related queries in SQL. Finally a for-loop is created to iterate over the columns to drop any rows using the `drop` function from Pandas. This is done using an if statement to select the rows to be dropped using the abnormal variables that don't fit in the columns as conditions. This process is repeated for the other tables.
```python
    # Cleaning of the legacy store details table 
    def clean_store_data(self):
        self.df_lsd['continent']=self.df_lsd['continent'].str.replace('ee', '')
        self.df_lsd['opening_date'] =pd.to_datetime(self.df_lsd['opening_date'], infer_datetime_format=True, errors='coerce')
        self.df_lsd['staff_numbers'] = self.df_lsd['staff_numbers'].str.replace(r'[a-zA-Z%]', '') # r'[a-zA-Z%]' represents non-numerical values that are to be removed in a column to contain only numerical values.
        for row in self.df_lsd.index:
            if self.df_lsd.loc[row, 'lat'] != None and self.df_lsd.loc[row, 'store_type'] != 'Web Portal':
                self.df_lsd.drop(row, inplace = True)
       
        print(self.df_lsd.head()) #prints the top 5 rows of the DataFrame
        return self.df_lsd
```

---
## ***Sending to PostgreSQL***
> Here the cleaned tables will be sent to the PostgreSQL database using SQLAlchemy and the code for it is ran from the main.py file.

This stage is where the cleaned data from the `DataClean` class in the data_cleaning.py file is sent to the PostgreSQL database. In the database_utils.py file, the following `upload_to_db` method is created in the `DatabaseConnector` class. This method will send the cleaned DataFrame (which it takes as an arguement) to the local PostgreSQL database where it can be accessed using pgadmin4 using SQLAlchemy. Here the `to_sql` function from SQLAlchemy will be used to send the DataFrame to PostgreSQL. The arguements given to this function in respective order are the name of the table, the engine to the local PostgreSQL server defined in the line before as `self.tosql` and finally, the configuration `if_exists` set to "replace" so that if the table needs to re-uploaded to PostgreSQL then the new table will overwrite the old table.
```python
    def upload_to_db(self, db_clean):
        self.local_type ='postgresql'
        self.local_api ='psycopg2'
        self.local_host = 'localhost'
        self.local_password = 'password'
        self.local_user = 'postgres'
        self.local_database = 'Sales_Data'
        self.local_port = '5432'
        self.tosql = ce(f"{self.local_type}+{self.local_api}://{self.local_user}:{self.local_password}@{self.local_host}:{self.local_port}/{self.local_database}")
        db_clean.to_sql('order_table',self.tosql, if_exists = 'replace')
```

However, if the `DataClean` class is imported to the database_utils.py file and the code above to send the DataFrame to PostgreSQL is run on this file, this will create a circular import error. This is due to the fact that the database_utils.py file will export it's class to the data_extraction.py file which will then export it's own class to the data_cleaning.py file which will also export it's class back to the database_utils.py, thus creating a loop of imports. To the avoid, the `upload_to_db` method is run on the different file called main.py.

The code below shows the main.py script. Here in this script, the `DatabaseConnector` and the `DataClean` classes are imported from their respective files and the instances of both classes are called. The variable `clean_df` is defined as the method of the `DataClean` class that has returned the respective cleaned DataFrame that is to be sent to PostgreSQL. Finally, the `upload_to_db` method of the `DatabaseConnector` will be called, with`clean_df` given to it as an arguement.

```python
from database_utils import DatabaseConnector
from data_cleaning import DataClean

if __name__ == '__main__':
    conn = DatabaseConnector()
    data_clean = DataClean()
    clean_df = data_clean.clean_orders_data()
    conn.upload_to_db(clean_df) 
```


---
## ***Creating the schema***
> This section of the project involves  casting the columns to the correct types, so when querying the tables to answer the business questions, the required aggregrations can be computed and the star based schema can be created using the columns of the orders_table as the primary keys.
### _Casting the Colums_
Below is the query to cast the columns of the products table using `ALTER COLUMN` function, to the correct types as they are cast as text types after being imported from python. It shows for example, the product_price and weight columns being cast to floats so calculations can be performed using them.

```SQL
ALTER TABLE dim_products
	ALTER COLUMN "product_price(£)" TYPE float4 USING "product_price(£)"::real,
	ALTER COLUMN "Weight(kg)" TYPE float4 USING "Weight(kg)"::real,
	ALTER COLUMN "EAN" TYPE VARCHAR(17),
	ALTER COLUMN date_added TYPE date,
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN uuid TYPE UUID USING uuid::uuid,
	ALTER COLUMN weight_class TYPE VARCHAR(14);
```
However, certain columns cannot be cast to a different type due to a certain variable in the column. For example, in the products table, the product_price column contained the '£' sign which prevented it from being able to be cast as a float column. So the query below is used to remove the '£' sign using the `REPLACE` function. Then that comun was renamed to "product_price(£)" so the reader knows the values in that column are in pounds. 

```SQL
UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '') 

ALTER TABLE dim_products
RENAME COLUMN product_price TO "product_price(£)"
```
Also a new column called 'weight_class' is added to the products table using `ADD COLUMN`. This new column is used to help the delivery team make decisions on how to handle the products with regards to the weight. Here, the `CASE` function is used to create human-readable values to the 'weight_class' column based the values of the 'Weight(kg)' column.
```SQL
ALTER TABLE dim_products
ADD COLUMN weight_class varchar(20)

UPDATE dim_products
SET weight_class = 
                    CASE WHEN ("Weight(kg)" > 140) THEN 'Truck_Required' 
                     WHEN ("Weight(kg)" BETWEEN 40 AND 140)  THEN 'Heavy' 
                     WHEN ("Weight(kg)" BETWEEN 2 AND 40)  THEN 'Mid_Sized' 
                     WHEN ("Weight(kg)" < 2) THEN 'Light' END
```
### _Adding the primary and foreign keys_
Finally to create the star-based schema, primarys keys are added to the columns of the orders table that are shared with the other tables. Then, foreign keys are added to the columns of the other tables that correspond to the orders table.  

---
## ***Querying the data***

### _How many stores does the business have and in which countries?_

The SQL query below shows how many stores in each country. GB has the most amount of stores with 265, then it is Germany with 141 and the US has 34 stores. There is also one store that is not based in a country due to it being online. In this query the `COUNT(*)` is used to compute the number of rows as the number of stores as each row represents a store. And ``GROUP BY` is used to group the number of stores for each country.
```SQL
SELECT country_code, COUNT(*) AS number_of_stores FROM dim_store_details 
GROUP BY country_code 
```
### _Which locations currently have the most stores?_

From using the following query, it can be deduced that the location with the most stores is Chapletown which has 14 stores. This query follows the same principle as the previous query, only this time, the `ORDER BY` function is used to order the rows from the highest number of stores to the lowest using `DESC`. 
```SQL
SELECT locality, COUNT(store_code) AS total_no_stores FROM dim_store_details 
GROUP BY locality ORDER BY COUNT(store_code) DESC
```
### _Which months produce the most sales typically?_

Using the following query, the highest number of sales are produced in August and the lowest is produced in February. This query uses `JOIN` to connect the order table with the product and date tables. It uses the `SUM` aggregration to add up the multiplied values of the product quantity in the orders table to the product price in the products table, and then grouped for each month in the date table using `GROUP BY`. 
 ```SQL
SELECT SUM(product_quantity * dim_products."product_price(£)") AS total_sales, dim_date_details.month FROM order_table 
JOIN dim_products
ON dim_products.product_code = order_table.product_code
JOIN dim_date_details
ON dim_date_details.date_uuid = order_table.date_uuid
GROUP BY dim_date_details.month
ORDER BY SUM(product_quantity * dim_products."product_price(£)") DESC
```
### _How many sales are coming from online?_

The query below shows that 107739 sales are coming from online. The query use `COUNT` to get the number of sales from the orders table. Then `CASE` is used to create a new column called location to show if the store is on the Web or offline based on the store code of the order.
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
### _What percentage of sales come through each type of store?_
The following query provides the percentages of sales for each store type, with the local store type having the highest. Once again, `JOIN` is used so the order table can be connected to the store and product tables. Aggregrations are also used to find the total sales likewise before and the percentage of sales is computed using the percentage formula. Also, this time `ROUND` is used to round to the total sales and the percentage of sales to 2 decimal places.
 ```SQL
SELECT dim_store_details.store_type,
ROUND(SUM(product_quantity * dim_products."product_price(£)"):: numeric, 2) AS total_sales,
ROUND(COUNT(*) * 100 / SUM(COUNT(*)) over():: numeric, 2) AS "percentage_total(%)"
FROM order_table
JOIN dim_products
ON dim_products.product_code = order_table.product_code
JOIN dim_store_details
ON dim_store_details.store_code = order_table.store_code
GROUP BY dim_store_details.store_type
ORDER BY COUNT(*) * 100 / SUM(COUNT(*)) over() DESC
```
### _Which month in each year produced the most sales?_
This query below shows which month of each year had the highest sales. In this query it can be seen that in 2022, the highest sales were obtained in April whereas in 2021, it was obtained in November. The query uses the `WITH` clause to create two auxillary statements. The first statement is `sales` which will find the total sales using the `SUM` aggregration, per year and month, by joing the orders table with the products table and the date table. The second auxillary statement called `sales_per_year`, which use the `MAX` aggregration to find the highest total sales in each year in a column called `max_sales`. Finally, outside of the `WITH` clause, a new statement is created to join both auxillary statements, to create a table to show the months in which the highest sales occur in each year.
 ```SQL
WITH sales AS
(SELECT ROUND(SUM(product_quantity * dim_products."product_price(£)") :: numeric, 2 ) AS total_sales, dim_date_details.year AS year,dim_date_details.month AS month
FROM order_table 
LEFT JOIN dim_products
ON dim_products.product_code = order_table.product_code
RIGHT JOIN dim_date_details
ON dim_date_details.date_uuid = order_table.date_uuid
GROUP BY dim_date_details.year, dim_date_details.month
)

,sales_per_year AS
(SELECT  MAX(total_sales) AS max_sales, year  FROM sales 
GROUP BY year
ORDER BY  year DESC 
)

SELECT max_sales, number_of_sales.year, number_of_sales.month FROM sales_per_year
JOIN number_of_sales
ON sales_per_year.max_sales = number_of_sales.total_sales
ORDER BY  year DESC
 ```
### _What is our staff headcount?_
This query shows the staff headcount for each country and it can be deduced that Britain has the highest number of staff. It uses `SUM` to add up the staff numbers for each country with `GROUP BY`. Then, some stores have 'N/A' has their country code due to the store no being based in a country as it's online. So the `CASE` function is used to change the store code from 'N/A' to 'WEB'.

 ```SQL
SELECT SUM(staff_numbers) AS total_staff_numbers,
CASE country_code WHEN 'N/A' Then 'WEB' ELSE country_code  END AS country_code
FROM dim_store_details
GROUP BY country_code ORDER BY SUM(staff_numbers) DESC
 ```
### _Which German store type is selling the most?_
This query below finds which store type in Germany sells the most and it shows that outlets have the highest sales. Likewise with previous tables, it uses `SUM` to find the total sales using the orders table joined to the products table. It is also joined to the store table to group the total sales to the store types, with the `WHERE` function used to ensure they are the ones from Gernany.
 ```SQL
SELECT ROUND(SUM(product_quantity * dim_products."product_price(£)") :: numeric, 2 )AS total_sales,
dim_store_details.store_type, dim_store_details.country_code FROM order_table 
FULL JOIN dim_products
ON dim_products.product_code = order_table.product_code
FULL JOIN dim_store_details
ON order_table.store_code = dim_store_details.store_code
WHERE country_code = 'DE'
GROUP BY dim_store_details.store_type, dim_store_details.country_code
ORDER BY ROUND(SUM(product_quantity * dim_products."product_price(£)") :: numeric, 2 ) ASC
 ```
### _How quickly is the company making sales?_
This final query is used to find how quickly the company is making sales. Firstly the `WITH` clause is to create an auxiliary statement called `sales_rate`.Then from the date table, this statment will compute the year and the difference between a string operator that combines the year, month, day and timestamp columns as a timestamp datatype and an offset of that string operator created using the `LEAD` function. `LEAD` will create a column where the value for each row are the same of the string operator in the following row creating an offset. The difference between the string operator and the offset is saved as the 'time_taken'.    
Then in a new query statement, the year and the average of the 'time_taken' column using the `AVG`  aggregration are called from the `sales_rate` auxiliary statement and grouped by the year using `GROUP BY`.

 ```SQL
WITH sales_rate AS 
(SELECT year, ((year|| '-'|| month ||'-'|| day ||' '|| timestamp)::timestamp -
(LEAD(year|| '-'|| month ||'-'|| day ||' '|| timestamp::time)
OVER (ORDER BY year|| '-'|| month ||'-'|| day ||' '|| timestamp::time DESC ))::timestamp)
AS time_taken
FROM dim_date_details)

SELECT year, AVG(time_taken) AS actual_time_taken FROM sales_rate
GROUP BY year
ORDER BY year DESC
 ```
