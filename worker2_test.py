import pandas as pd
import os
from dags.data.postgredb import DataWareHouse
from dags.data.mongodb import DataLake
import psycopg2.errors
from datetime import datetime


if not os.path.exists(os.getcwd()+ "/dags/data/csv"):
    os.mkdir(os.getcwd()+ "/dags/data/csv")


import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DELIMITER = ";"

def extract_distinct_shop(last_run) -> list:
    DL = DataLake(role='read_only')
    return list(DL.products.find({'fetched_time': {'$gte': last_run}}, {"_id": 0}).sort([('fetched_time', -1), ('updated_at', -1)]).distinct('shop_id'))

def extract_product_from_shop(shop_id: int, last_run) -> list:
    DL = DataLake(role='read_only')
    return list(DL.products.find({'fetched_time': {'$gte': last_run}, 'shop_id': shop_id}, {"_id": 0}, allow_disk_use=True).sort([('fetched_time', -1)]))

def extract_product_from_shops(shop_ids: list, last_run) -> list:
    DL = DataLake(role='read_only')
    return list(DL.products.find({'fetched_time': {'$gte': last_run}, 'shop_id': {'$in': shop_ids}}, {"_id": 0}, allow_disk_use=True).sort([('fetched_time', -1)]))

def extract(last_run: float) -> list:
    DL = DataLake(role='read_only')
    return list(DL.products.find({'fetched_time': {'$gte': last_run}}, {"_id": 0}).sort([('fetched_time', -1), ('updated_at', -1)]))

def transform(extracted_product: list) -> list:
    path = os.getcwd()+ "/dags/data/csv/"
    INDEXING = False

    # Data pre-processing
    df = pd.DataFrame(extracted_product)
    df["product_price"] = df["product_price"].div(100000)
    df.astype(str).drop_duplicates(inplace=True, keep='first')
    df.replace(r';',  ',', regex=True, inplace=True)
    df.replace(r'\n',  ' ', regex=True, inplace=True)
    
    def transform_general(keys: list, table_name: str, sub_name: str="", strip_key: list=[], expand: dict={}, expand_inplace: bool=False, replace_column_value: list=[]) -> dict: 
        file_name = f"{table_name}{sub_name}.csv"
        file_path = path + file_name
        
        # data proccessing
        data = df.filter(items=keys).astype(str).drop_duplicates()
        for key in strip_key:
            data[key].replace({r'\s+$': '', r'^\s+': ''}, regex=True, inplace=True)
            data[key].replace(' ',  '', regex=True, inplace=True)

        if replace_column_value:
            def transform_column(x):
                return x.replace(value['old_value'], value['new_value'])

            for column in replace_column_value:
                for column_name, values in column.items():
                    for value in values:
                        data[column_name] = data[column_name].apply(lambda x: pd.Series(transform_column(x)))

        if expand:
            keys.extend(expand['new_key'])
            if table_name == "product_time":
                def to_date(x) -> datetime:
                    return datetime.utcfromtimestamp(float(x))
                old_cols = data[expand['old_key']]
                data['day'] = old_cols.apply(lambda x: pd.Series(to_date(x).day))
                data['month'] = old_cols.apply(lambda x: pd.Series(to_date(x).month))
                data['year'] = old_cols.apply(lambda x: pd.Series(to_date(x).year))
                data['datetime'] = old_cols.apply(lambda x: pd.Series(to_date(x)))
            if expand_inplace:
                data.drop(columns=expand['old_key'], inplace=expand_inplace)
                keys.remove(expand['old_key'])


        data.to_csv(file_path, index=INDEXING, sep=DELIMITER)
        return {'file_path': file_path, 'table_name': table_name, 'keys': keys}

    shop = transform_general(
        keys = ["shop_id", "fetched_time", "shop_location", "shopee_verified", "is_official_shop"],
        table_name = "shop",
        replace_column_value = [
            {'shopee_verified': [
                {"old_value": "None", "new_value": 'False'}
            ]},
            {'is_official_shop': [
                {"old_value": "None", "new_value": 'False'}
            ]},
        ]
    )
    product = transform_general(
        keys = ["product_id", "fetched_time", "product_name", "product_image", "product_link", "updated_at", "shop_id"],
        table_name = "product",
        strip_key = ["product_image", "product_link"]
    )
    product_brand = transform_general(
        keys = ["product_id", "fetched_time", "product_brand", "category_id", "label_ids"],
        table_name = "product_brand",
        replace_column_value = [
            {"label_ids": [
                    {"old_value": "[", "new_value": "{"},
                    {"old_value": "]", "new_value": "}"}
                ]
            }
        ]
    )
    product_price = transform_general(
        keys = ["product_id", "fetched_time", "product_price", "product_discount", "currency", "is_freeship", "is_on_flash_sale"],
        table_name= 'product_price'
    )
    product_rating = transform_general(
        keys = ["product_id", "fetched_time", "rating_star", "rating_count" , "rating_with_context", "rating_with_image"],
        table_name = "product_rating",
        replace_column_value = [
            {"rating_count": [
                    {"old_value": "[", "new_value": "{"},
                    {"old_value": "]", "new_value": "}"}
                ]
            }
        ]
    )
    product_feedback = transform_general(
        keys = ["product_id", "fetched_time", "feedback_count", "liked_count", "view_count"],
        table_name = "product_feedback"
    )
    product_quantity = transform_general(
        keys = ["product_id", "fetched_time", "sold", 'stock'],
        table_name = "product_quantity"
    )
    product_time = transform_general(
        keys = ["product_id", "fetched_time"],
        table_name = "product_time",
        expand = {
            "old_key": "fetched_time",
            "new_key": ["day", "month", "year", "datetime"]
        },
        expand_inplace = False
    )
    return [shop, product, product_brand, product_price, product_rating, product_feedback, product_quantity, product_time]
        
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
            else:
                os.remove(file_path)
                pass
            finally:
                # os.remove(file_path)
                pass
        return True
    except Exception as e:
        print(e)

if __name__ == "__main__":
    last_run = 1622101129.284765
    shop_ids = extract_distinct_shop(last_run)

    def etl(shop_ids, last_run):
        products = extract_product_from_shops(shop_ids, last_run)
        print("number of product", len(products))
        transformed = transform(products)
        loading = load(transformed)
        print("Loading: ", loading)

    a = len(shop_ids)
    print("Number of shops: ", a)
    limit = 1000
    for i in range(0, a, limit):
        shops = shop_ids[i:i+limit]
        print(f"{i}. Number of shop: ", len(shops))
        etl(shops, last_run)