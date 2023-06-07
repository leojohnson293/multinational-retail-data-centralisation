import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine as ce
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from PyPDF2 import PdfFileReader
import tabula
import requests
import json
import boto3
import yaml


class DataExtractor: 
    def __init__(self):
        db_con = DatabaseConnector()
        self.engine = db_con.init_db_engine()
        pd.set_option('display.max_rows', None)
        self.legacy_users = None
        self.df_legacy_users = None
        self.pdf ='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        keys = r'C:\Users\acer laptop\Desktop\AiCore\leo_accessKeys.yaml'
        with open(keys) as f:
            self.aws_keys = yaml.safe_load(f)

    def read_RDS_data(self):
        self.orders_table = self.engine.execute('''SELECT * FROM orders_table''').fetchall()
        #print(self.orders_table)
        return self.orders_table

    def extract_RDS_table(self):
        self.df_orders_table =pd.read_sql_table('orders_table', self.engine)
       # print(self.df_orders_table.head())
        return self.df_orders_table

    def retrieve_pdf_data(self):
        self.card_details=tabula.read_pdf(self.pdf, lattice=True, pages= 'all', stream=True)
        tabula.convert_into(self.pdf, "output.csv", output_format="csv", pages='all')
        self.pd_card_details = pd.read_csv('new_output.csv') 
        return self.pd_card_details

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
        print(self.df_stores)
        return self.df_stores


    def extract_from_s3(self):
        client = boto3.client('s3',aws_access_key_id= self.aws_keys['Access key ID'], aws_secret_access_key=self.aws_keys['Secret access key'])
        csv_object = client.get_object(Bucket = 'data-handling-public', Key = 'products.csv' )['Body']
        csv_object = csv_object.read().decode('utf-8')
        from io import StringIO
        df_product_details = pd.read_csv(StringIO(csv_object))
        return df_product_details

        #print((self.df_product_details))

    def extract_json_data(self):
        client = boto3.client('s3',aws_access_key_id=self.aws_keys['Access key ID'], aws_secret_access_key=self.aws_keys['Secret access key'])
        json_object = client.get_object(Bucket = 'data-handling-public', Key = 'date_details.json' )['Body']
        json_object = json_object.read().decode('utf-8')
        from io import StringIO
        df_date_details = pd.read_json(StringIO(json_object))
        #print(df_date_details)
        #df_date_details.to_csv('s3_json.csv', encoding='utf-8', index=False)
        return df_date_details


if __name__ == '__main__':
    ins = DataExtractor()
    ins.retrieve_pdf_data()
                                
    


