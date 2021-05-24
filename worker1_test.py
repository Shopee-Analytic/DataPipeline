import requests
import re
from datetime import datetime
from random import uniform
import time
from dags.data.mongodb import DataLake
import yaml
import os
import concurrent.futures



import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def retry_with_backoff(retries=4, backoff_in_seconds=1):
    def rwb(get_data):
        def wrapper(url):
            x = 0
            while True:
                try:
                    return get_data(url)
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

def get_category_id(url_of_category) -> int:
    return re.search(r'https://shopee.vn/.+-cat.(\d+)', url_of_category).group(1)

def get_url(category_id, newest) -> str:
    limit = 100
    return 'https://shopee.vn/api/v4/search/search_items?by=relevancy&limit={}&match_id={}&newest={}&order=desc&page_type=search&version=2'.format(limit, category_id, newest)

@retry_with_backoff()
def get_data(url) -> dict:
    return requests.get(url, headers={'content-type': 'text'}, timeout=10).json()

def extract(link, newest):
    category_id = get_category_id(link)
    url = get_url(category_id, newest)
    data = get_data(url)
    return data

def transform(new_data):  # data = [{}, {}, {}, ...]
    data = []
    try:
        items = new_data['items']
        for item in items:
            item = item['item_basic']
            data.append(
                {
                    'product_id': item['itemid'],
                    'product_name': item['name'],
                    'product_image': r'https://cf.shopee.vn/file/{}_tn'.format(item['image']),
                    'product_link': r'https://shopee.vn/{}-i.{}.{}'.format(item['name'], item['shopid'], item['itemid']),
                    'category_id': item['catid'],
                    'product_price': item['price'],
                    'product_discount': item['raw_discount'],
                    'currency': item['currency'],
                    'stock': item['stock'],
                    'sold': item['sold'],
                    'rating_star': item['item_rating']['rating_star'],
                    'rating_count': item['item_rating']['rating_count'],
                    'feedback_count': item['cmt_count'],
                    'shop_id': item['shopid'],
                    'shop_location': item['shop_location'],
                    'shopee_verified': item['shopee_verified'],
                    'updated_at': item['ctime'],
                    'fetched_time': datetime.timestamp(datetime.utcnow())
                }
            )
        return data
    except (TypeError, Exception):
        return None

def load(transformed_data):
    datalake = DataLake(role='read_and_write')
    return datalake.insert_many_products(transformed_data)

if __name__ == "__main__":
    def etl(link, page):
        extracted_data = extract(link, page)
        transformed_data = transform(extracted_data)
        return load(transformed_data)

    with open(f'{os.getcwd()}/dags/config/config.yml') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        links = data['links']
        pages = data['pages']

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for link in links:
            for page in range(0, pages, 1):
                futures.append(executor.submit(etl, link, page))

    for future in concurrent.futures.as_completed(futures):
        logger.info(len(future.result()))
        