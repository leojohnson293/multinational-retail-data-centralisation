from database_utils import DatabaseConnector
from data_cleaning import DataClean

# dim users
if __name__ == '__main__':
    conn = DatabaseConnector()
    data_clean = DataClean()
    clean_df = data_clean.clean_user_data()
    conn.upload_to_db(clean_df)

# # dim card details
# if __name__ == '__main__':
#     conn = DatabaseConnector()
#     data_clean = DataClean()
#     clean_df = data_clean.clean_card_data()
#     conn.upload_to_db(clean_df)

# # dim store details
# if __name__ == '__main__':
#     conn = DatabaseConnector()
#     data_clean = DataClean()
#     clean_df = data_clean.clean_store_data()__
#     conn.upload_to_db(clean_df)

# #dim_products
# if __name__ == '__main__':
#     conn = DatabaseConnector()
#     data_clean = DataClean()
#     clean_df = data_clean.convert_product_weights()
    # conn.upload_to_db(clean_df)

# orders table
if __name__ == '__main__':
    conn = DatabaseConnector()
    data_clean = DataClean()
    clean_df = data_clean.clean_orders_data()
    conn.upload_to_db(clean_df)

# # dim_date_times
# if __name__ == '__main__':
#     conn = DatabaseConnector()
#     data_clean = DataClean()
#     clean_df = data_clean.clean_date_data()
#     conn.upload_to_db(clean_df)