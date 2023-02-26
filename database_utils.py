import yaml 
import pandas as pd
import psycopg2
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
        return self.engine#

    def list_db_tables(self):
        self.inspector = inspect(self.engine)
        print(self.inspector.get_table_names())

    def upload_to_db(self, db_clean):
        self.local_type ='postgresql'
        self.local_api ='psycopg2'
        self.local_host = 'localhost'
        self.local_password = 'password'
        self.local_user = 'postgres'
        self.local_database = 'Sales_Data'
        self.local_port = '5432'
        self.tosql = ce(f"{self.local_type}+{self.local_api}://{self.local_user}:{self.local_password}@{self.local_host}:{self.local_port}/{self.local_database}")
        # db_clean.to_sql('dim_products',self.tosql, if_exists = 'replace')
        db_clean.to_sql('order_table',self.tosql, if_exists = 'replace')




