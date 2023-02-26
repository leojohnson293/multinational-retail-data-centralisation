import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine as ce
from sqlalchemy import inspect
from data_extraction import DataExtractor
import datetime



class DataClean:
    def __init__(self):
        db_ext = DataExtractor()
        self.df_rds = db_ext.extract_RDS_table()
        self.df_cd = db_ext.retrieve_pdf_data()
        self.df_lsd = db_ext.retrieve_stores_data()
        self.df_prod = db_ext.extract_from_s3()
        self.df_date = db_ext.extract_json_data()
        pd.set_option('display.max_rows', None)

    def clean_user_data(self):
        self.df_lu['country_code']= self.df_lu['country_code'].str.replace('GGB', 'GB')
        self.df_lu['date_of_birth'] =pd.to_datetime(self.df_lu['date_of_birth'], infer_datetime_format=True, errors='coerce')
        self.df_lu['phone_number'] = self.df_lu['phone_number'].str.replace('x', '')
        for row in self.df_lu.index:
            if self.df_lu.loc[row, 'country_code'] == 'NULL':
                 self.df_lu.drop(row, inplace = True)
        country = ['Germany', 'United Kingdom', 'United States']
        for row in self.df_lu.index:
            if self.df_lu.loc[row, 'country'] not in country:
                 self.df_lu.drop(row, inplace = True)
        return self.df_lu

    def clean_card_data(self):
        self.df_cd['date_payment_confirmed'] =pd.to_datetime(self.df_cd['date_payment_confirmed'], infer_datetime_format=True, errors='coerce')
        self.df_cd=self.df_cd.dropna()
        self.df_cd['card_number'] = self.df_cd['card_number'].str.replace('?', '')

        print(self.df_cd[167:171])
        return self.df_cd

    def clean_store_data(self):
        self.df_lsd['continent']=self.df_lsd['continent'].str.replace('ee', '')
        self.df_lsd['opening_date'] =pd.to_datetime(self.df_lsd['opening_date'], infer_datetime_format=True, errors='coerce')
        self.df_lsd['staff_numbers'] = self.df_lsd['staff_numbers'].str.replace(r'[a-zA-Z%]', '')
        for row in self.df_lsd.index:
            if self.df_lsd.loc[row, 'lat'] != None and self.df_lsd.loc[row, 'store_type'] != 'Web Portal':
                self.df_lsd.drop(row, inplace = True)

        print(self.df_lsd.head())
        return self.df_lsd

    def convert_product_weights(self):
        self.df_prod = self.df_prod.dropna()
        self.df_prod['weight']=self.df_prod['weight'].str.replace(' x ', '*')
        self.df_prod['weight']=self.df_prod['weight'].str.replace('kg', ' k')
        self.df_prod['weight']=self.df_prod['weight'].str.replace('g', ' g')
        self.df_prod['weight']=self.df_prod['weight'].str.replace('ml', ' ml')
        self.df_prod['weight']=self.df_prod['weight'].str.replace('oz', ' oz')
        self.df_prod[['Weight(kg)','Units']] = self.df_prod.weight.str.split(" ",1,expand=True,)
        self.df_prod['date_added'] =pd.to_datetime(self.df_prod['date_added'], infer_datetime_format=True, errors='coerce')
        self.df_prod['Weight(kg)'] = pd.to_numeric(self.df_prod['Weight(kg)'],errors='coerce')
        self.df_prod['Units']=self.df_prod['Units'].str.replace('.', '')
        self.df_prod['Units']=self.df_prod['Units'].str.replace(' ', '')
        self.df_prod['Weight(kg)'] = self.df_prod['Weight(kg)'].round(3) #Rounds its to three decimal places
        self.df_prod[['m1','m2']] = self.df_prod.weight.str.split("*",1,expand=True,)
        self.df_prod['m2']=self.df_prod['m2'].str.replace('g', '')

        for weight_in_kg in self.df_prod.index:
            if '*' in self.df_prod.loc[weight_in_kg, 'weight']:
                self.df_prod['m1'] = pd.to_numeric(self.df_prod['m1'],errors='coerce')
                self.df_prod['m2'] = pd.to_numeric(self.df_prod['m2'],errors='coerce')
                self.df_prod.loc[weight_in_kg, 'Weight(kg)'] = self.df_prod.loc[weight_in_kg, 'm1'] * self.df_prod.loc[weight_in_kg, 'm2']
        for weight_in_kg in self.df_prod.index:
            if self.df_prod.loc[weight_in_kg,'Units'] == 'g':
                self.df_prod.loc[weight_in_kg,'Weight(kg)'] = self.df_prod.loc[weight_in_kg,'Weight(kg)']/1000
        for weight_in_kg in self.df_prod.index:
            if self.df_prod.loc[weight_in_kg,'Units'] == 'ml':
                self.df_prod.loc[weight_in_kg,'Weight(kg)'] = self.df_prod.loc[weight_in_kg,'Weight(kg)']/1000
        for weight_in_kg in self.df_prod.index:
            if self.df_prod.loc[weight_in_kg,'Units'] == 'oz':
                self.df_prod.loc[weight_in_kg,'Weight(kg)'] = self.df_prod.loc[weight_in_kg,'Weight(kg)']/35.274      
        
        del self.df_prod['weight']
        del self.df_prod['Units']
        del self.df_prod['m1']
        del self.df_prod['m2']
        self.df_prod = self.df_prod.dropna(thresh = 10)

        print(self.df_prod)
        return self.df_prod

    def clean_orders_data(self):
        del self.df_rds['first_name']
        del self.df_rds['last_name']
        del self.df_rds['1']

        print(self.df_rds.head())
        return self.df_rds

    def clean_date_data(self):
        for row in self.df_date.index:
            if self.df_date.loc[row, 'day'] == 'NULL':
                 self.df_date.drop(row, inplace = True)
        #pd.options.display.float_format = '{:,.0f}'.format
        self.df_date['month'] = pd.to_numeric(self.df_date['month'],errors='coerce')
        self.df_date = self.df_date.dropna()
        self.df_date['month'] = self.df_date['month'].astype(np.int64)
        #print(self.df_date[7575:7586])
        #print(type(self.df_date.loc[8343, 'timestamp']))
        return self.df_date


if __name__ == '__main__':
    ins = DataClean()
    ins.clean_store_data()



