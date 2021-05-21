import pandas as pd
import os
from data.postgredb import DataWareHouse
from data.mongodb import DataLake
import psycopg2.errors

if not os.path.exists(os.getcwd()+ "/dags/data/csv"):
    os.mkdir(os.getcwd()+ "/dags/data/csv")


import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DELIMITER = "@"

def extract_distinct_shop(last_run) -> list:
    DL = DataLake(role='read_only')
    return list(DL.products.find({'fetched_time': {'$gte': last_run}}, {"_id": 0}).sort([('fetched_time', -1), ('updated_at', -1)]).distinct('shop_id'))

def extract_product_from_shop(shop_id: int, last_run) -> list:
    DL = DataLake(role='read_only')
    return list(DL.products.find({'fetched_time': {'$gte': last_run}, 'shop_id': shop_id}, {"_id": 0}).sort([('fetched_time', -1), ('updated_at', -1)]))

def transform(extracted_product) -> list:
    path = os.getcwd()+ "/dags/data/csv/"
    INDEXING = False

    df = pd.DataFrame(extracted_product).astype(str).drop_duplicates()

    def transform_general(keys: list, table_name: str, sub_name: str="") -> dict:
        file_name = f"{table_name}"
        if sub_name != "":
            file_name += f"_{sub_name}.csv"
        else:
            file_name += ".csv"
        file_path = path + file_name
        
        data = df.filter(items=keys).astype(str).drop_duplicates()
        for key in keys:
            data[key] = data[key].replace(" ", "")
        data.to_csv(file_path, index=INDEXING, sep=DELIMITER)
        return {'file_path': file_path, 'table_name': table_name, 'keys': keys}

    shop = transform_general(
        keys = ["shop_id", "fetched_time", "shop_location", "shopee_verified"],
        table_name = "shop"
    )
    product = transform_general(
        keys = ["product_id", "fetched_time", "product_name", "product_image", "product_link", "category_id", "updated_at", "shop_id"],
        table_name = "product"
    )
    product_price = transform_general(
        keys = ["product_id", "fetched_time", "product_price", "product_discount", "currency"],
        table_name= 'product_price'
    )
    # product_rating = transform_general(
    #     keys = ["product_id", "fetched_time", "rating_star", "rating_count_1", "rating_count_2", "rating_count_3", "rating_count_4", "rating_count_5"],
    #     table_name = "product_rating"
    # )
    product_feedback = transform_general(
        keys = ["product_id", "fetched_time", "feedback_count"],
        table_name = "product_feedback"
    )
    product_quantity = transform_general(
        keys = ["product_id", "fetched_time", "sold", 'stock'],
        table_name = "product_quantity"
    )
    # product_time = transform_general(
    #     keys = ["product_id", "fetched_time", ""]
    # )
    return [shop, product, product_price, product_feedback, product_quantity]

        
def load(transformed_data):
    DWH = DataWareHouse(role='admin')

    try:
        for data in transformed_data:
            try:
                keys = data['keys']
                file_path = data['file_path']
                table_name = data['table_name']
                DWH.copy_data_by_csv(file_path=file_path, table_name=table_name, keys=keys, delimiter=DELIMITER)
            except psycopg2.errors.UniqueViolation as e:
                logger.error(e)
                continue
            finally:
                os.remove(file_path)
        return True
    except Exception as e:
        logger.error(e)
        raise e

    
