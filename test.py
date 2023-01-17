import boto3
import pandas as pd

def extract_from_s3():
    client = boto3.client('s3')
    s3_path= 's3://data-handling-public/products.csv'
    df_product_details = pd.read_csv(s3_path)

    print(df_product_details.head())


extract_from_s3()