import pandas as pd
import psycopg2
from sqlalchemy import create_engine as ce
from sqlalchemy import inspect
from data_extraction import DataExtractor
import datetime

db_ext = DataExtractor()
df_lu = db_ext.extract_users_table()
df_cd = db_ext.retrieve_pdf_data()
pd.set_option('display.max_rows', None)

class DataClean:

    def clean_user_data(self):
        df_lu['country_code']= df_lu['country_code'].str.replace('GGB', 'GB')
        df_lu['date_of_birth'] =pd.to_datetime(df_lu['date_of_birth'], infer_datetime_format=True, errors='coerce')
        df_lu['phone_number'] = df_lu['phone_number'].str.replace('x', '')
        for x in df_lu.index:
            if df_lu.loc[x, 'country_code'] == 'NULL':
                 df_lu.drop(x, inplace = True)
        country = ['Germany', 'United Kingdom', 'United States']
        for x in df_lu.index:
            if df_lu.loc[x, 'country'] not in country:
                 df_lu.drop(x, inplace = True)
        return df_lu

    def clean_card_data(self):
        print(df_cd['Unnamed: 0'])




