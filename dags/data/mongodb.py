import pymongo
import yaml
import logging
from random import uniform
import time
import os


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def retry_getclient_with_backoff(retries=4, backoff_in_seconds=1):
    def rwb(get_client):
        def wrapper(role):
            x = 0
            while True:
                try:
                    return get_client(role)
                except:
                    if x == retries:    
                        raise
                    else:
                        sleep = (backoff_in_seconds * 2 ** x +
                                 uniform(0, 1))
                        time.sleep(sleep)
                        x += 1
        return wrapper
    return rwb

@retry_getclient_with_backoff()
def get_client(role):
    with open(f"{os.getcwd()}/dags/config/db-config.yml") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        client = data['mongo'][role]
    return pymongo.MongoClient(client['url'])

# Version 1 - Data in 1 collection
class DataLake:
    def __init__(self, role='read_and_write', database="Crawler", collection='shopee'):
        client = get_client(role=role)
        self.mydb = client[database]
        self.products = self.mydb[collection]

    def create_index(self, indexes: list):
        try:
            for index in indexes:
                self.products.create_index(
                    [
                    (index['key'], index['index_type']), 
                    ])
        except Exception as e:
            logger.error(e)
        else:
            logger.info("Create index on DATALAKE success")

    def drop_index(self):
        try:
            self.products.drop_indexes()
            logger.info('DROP indexes successfully')
        except Exception as e:
            logger.error(e)

    def insert_one_product(self, product_data: dict) -> str:
        return str(self.products.insert_one(product_data).inserted_id)

    def insert_many_products(self, product_data: list) -> list:
        ids = []
        try:
            for _id in self.products.insert_many(product_data).inserted_ids:
                ids.append(str(_id))
        except Exception as e:
            logger.error(e)
            for data in product_data:
                ids.append(self.insert_one_product(data))
        return ids

    def find_one_by_id(self, product_id) -> dict:
        return self.products.find_one({"product_id": product_id})

    def find_duplicates(self):
        return list(self.products.aggregate([
            {"$group" : { "_id": "$product_id", "count": { "$sum": 1 } } },
            {"$match": {"_id" :{ "$ne" : 'null' } , "count" : {"$gt": 1} } }, 
            {"$project": {"product_id" : "$_id", "_id" : 0} }
        ]))
if __name__ == "__main__":
    indexes = [
        {"key": "_id", "index_type": 1},
        {"key": "fetched_time", "index_type": -1},
        {"key": "updated_at", "index_type": -1}
    ]
    DL = DataLake(role='read_and_write')
    DL.drop_index()
    DL.create_index(indexes=indexes)
    # print(len(list(DL.products.find())))
    # print(DL.find_duplicates())

    