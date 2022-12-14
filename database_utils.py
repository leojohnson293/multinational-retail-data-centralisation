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
        # print(self.read_db_creds)  
        
    
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

    # def upload_to_db(self):
    #     from data_cleaning import DataClean
    #     db_clean = DataClean()
    #     clean_user = db_clean.clean_user_data()
    #     clean_user.to_sql('legacy_users', self.engine, if_exists='replace')



# ins = DatabaseConnector()
# ins.init_db_engine()
# ins.list_db_tables()
# ins.upload_to_db()


