import pymongo
import yaml
from datetime import datetime
import concurrent.futures
import logging
from random import uniform
import time


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def retry_getclient_with_backoff(retries=4, backoff_in_seconds=1):
    def rwb(get_client):
        def wrapper(role='read_and_write'):
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
    with open("config/db-config.yml") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        client = data['mongo'][role]
    return pymongo.MongoClient(client['url'])

# Version 1 - Data in 1 collection
class ShopeeCrawlerDB:
    def __init__(self, role='read_and_write'):
        client = get_client(role)
        mydb = client['ShopeeCrawler']
        self.products = mydb['shopee']

    def insert_one_product(self, product_data):
        product = self.find_one_by_id(product_id=product_data['_id'])
        if product is None:
            return self.products.insert_one(product_data).inserted_id
        else:
            if self.is_same(product, product_data, "fetched_time"):
                return None
            else:
                product_data["updated_at"] = product_data.pop("fetched_time")
                return self.products.update_one({"_id": product_data["_id"]}, {"$set": product_data}, upsert=True).upserted_id # Check if product existed -> overwrite <> Handle duplicated data

    def insert_many_products(self, product_data):
        futures = []
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                for product in product_data:
                    futures.append(executor.submit(self.insert_one_product, product))

            return [future.result() for future in concurrent.futures.as_completed(futures) if future.result() is not None]
        except TypeError:
            return []

    def find_one_by_id(self, product_id) -> dict:
        return self.products.find_one({"_id": product_id})

    def is_same(product_old, product_new, key_ignore) -> bool:
        return all(product_old[key] == product_new[key] for key in product_old.keys() if key != key_ignore)