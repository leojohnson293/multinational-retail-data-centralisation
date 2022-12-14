import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine as ce
from sqlalchemy import inspect
from database_utils import DatabaseConnector
import tabula

db_con = DatabaseConnector()
engine = db_con.init_db_engine()
pd.set_option('display.max_rows', None)

class DataExtractor: 

    def read_RDS_data(self):
        self.legacy_users = engine.execute('''SELECT * FROM legacy_users''').fetchall()
        #print(self.legacy_users)
        return self.legacy_users

    def extract_users_table(self):
        self.df_legacy_users =pd.read_sql_table('legacy_users', engine)
        #print(self.df_legacy_users)
        return self.df_legacy_users

    def retrieve_pdf_data(self):
        self.df_card_details = tabula.read_pdf('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', pages = [1,2,3])
        #tabula.convert_into('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf', 'output.csv', output_format = 'csv', pages = 'all')
        print(self.df_card_details)
        return self.df_card_details




ins = DataExtractor()
# ins.read_RDS_data()
# ins.extract_users_table()
# print(ins.df_legacy_users)   
ins.retrieve_pdf_data()                              
#print(ins.df_card_details)


