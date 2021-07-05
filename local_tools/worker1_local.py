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
                    'label_ids': item['label_ids'],
                    'product_brand': item['brand'],
                    'product_price': item['price'] if item['raw_discount'] == 0 else item['price_before_discount'],
                    'product_discount': item['raw_discount'],
                    'currency': item['currency'],
                    'stock': item['stock'],
                    'sold': item['sold'],
                    'is_on_flash_sale': item['is_on_flash_sale'],
                    'rating_star': item['item_rating']['rating_star'],
                    'rating_count': item['item_rating']['rating_count'],
                    'rating_with_context': item['item_rating']['rcount_with_context'],
                    'rating_with_image': item['item_rating']['rcount_with_image'],
                    'is_freeship': item['show_free_shipping'],
                    'feedback_count': item['cmt_count'],
                    'liked_count': item['liked_count'],
                    'view_count': item['view_count'],
                    'shop_id': item['shopid'],
                    'shop_location': item['shop_location'],
                    'shopee_verified': item['shopee_verified'],
                    'is_official_shop': item['is_official_shop'],
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

def indexing(indexes: list=[{"key": "_id", "index_type": 1}, {"key": "fetched_time", "index_type": -1}, {"key": "updated_at", "index_type": -1}]):
    try:
        datalake = DataLake(role='read_and_write')
        datalake.create_index(indexes=indexes)
        return True
    except Exception as e:
        logger.error(e)
        return False

def drop_index():
    DataLake(role='read_and_write', database="Crawler", collection='shopee').drop_index()

def start(links: list=[], pages: int=0):
    with open('local_tools/last_run.txt', "w") as f:
        f.write(str(datetime.timestamp(datetime.utcnow())))

    drop_index()

    def etl(link, page: int):
        extracted_data = extract(link, newest=page*100)
        transformed_data = transform(extracted_data)
        return load(transformed_data)

    if not (len(links) > 0 and pages > 0):
        with open(f'{os.getcwd()}/dags/config/config.yml') as f:
            data = yaml.load(f, Loader=yaml.FullLoader)
            links = data['links']
            pages = data['pages']

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for link in links:
            for page in range(0, pages, 1):
                futures.append(executor.submit(etl, link, page))
    count = 0
    for future in concurrent.futures.as_completed(futures):
        count += len(future.result()) if len(future.result()) else 0
    
    logger.info(count)
    indexing()

if __name__ == "__main__":
    start()