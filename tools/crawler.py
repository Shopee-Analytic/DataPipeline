import requests
import json
import re
from datetime import datetime
import random
import time
import logging
import logging.config

logging.config.fileConfig('config/logging.conf')

# create logger
logger = logging.getLogger('crawler')

def retry_with_backoff(retries=4, backoff_in_seconds=1):
    def rwb(crawl):
        def wrapper(url_of_category, newest):
            x = 0
            
            while True:
                try:
                    
                    return crawl(url_of_category, newest)
                except:
                    if x == retries:
                        logger.error(f"Out of retries, can't crawl from page {int(newest/100)+1} of {url_of_category}.")
                        raise
                    else:
                        logger.warning(f"{x+1}-retry to crawl from page {int(newest/100)+1} of {url_of_category}.")
                        sleep = (backoff_in_seconds * 2 ** x +
                                 random.uniform(0, 1))
                        time.sleep(sleep)
                        x += 1
        return wrapper
    return rwb


@retry_with_backoff()
def crawl(url_of_category, newest) -> dict:
    
    limit = 100  # Can only crawl 100 row each request
    # Category of the search link
    category_id = get_category_id(url_of_category)
    url = get_url(limit, category_id, newest)
    data = requests.get(url, headers={"content-type": "text"}, timeout=5)
    logger.info(f"Crawl from page {int(newest/100)+1} of {url_of_category} successfully.")
    return select_properties(data.json())


def crawl_to_file(url_of_category, file_output):

    limit = 100  # Can only crawl 100 row each request
    # Category of the search link
    category_id = get_category_id(url_of_category)
    newest = 0
    url = get_url(limit, category_id, newest)
    data = requests.get(url, headers={"content-type": "text"}, timeout=5)
    try:
        assert save_data_to_file(file_output, select_properties(
            data.json()[0])), f"Can't crawl data from this page:\n{url}\n"
    except AssertionError as msg:
        print(msg)
    return True


def get_url(limit, category_id, newest):
    return 'https://shopee.vn/api/v4/search/search_items?by=relevancy&limit={}&match_id={}&newest={}&order=desc&page_type=search&version=2'.format(limit, category_id, newest)


def get_category_id(url_of_category):
    # Test if web response
    if requests.get(url_of_category).status_code != 200:
        return None

    # Using regex to match the category_id
    category_id = re.search(r'https://shopee.vn/.+-cat.(\d+)', url_of_category).group(1)
    if not category_id:
        return None

    return category_id


def select_properties(new_data):  # data = [{}, {}, {}, ...]
    data = []

    items = new_data["items"]
    for item in items:
        item = item["item_basic"]
        data.append(
            {
                "_id": item['itemid'],
                "shop_id": item["shopid"],
                "product_name": item["name"],
                "category_id": item['catid'],
                "image": r"https://cf.shopee.vn/file/{}_tn".format(item["image"]),
                "currency": item['currency'],
                "stock": item['stock'],
                "sold": item['sold'],
                "price": item['price'],
                "discount": item['raw_discount'],
                "rating_star": item['item_rating']['rating_star'],
                "rating_count": item['item_rating']['rating_count'],
                "feedback_count": item['cmt_count'],
                "shop_location": item["shop_location"],
                "shopee_verified": item["shopee_verified"],
                "product_link": r"https://shopee.vn/{}-i.{}.{}".format(item['name'], item['shopid'], item['itemid']),
                "fetched_timestamp": datetime.timestamp(datetime.utcnow())
            }
        )
    return data


def save_data_to_file(file_output, new_data):  # data = [{}, {}, {}, ...]
    if not file_output.endswith(".json"):
        return False
    try:
        # Check if file already exists and append data
        with open(f'data/{file_output}', 'r+') as f_read:
            data = json.load(f_read)

            data += new_data
        with open(f'data/{file_output}', 'w+') as f_write:
            json.dump(data, f_write, indent=4)

        print(f"\tappend {len(new_data)} new data to data\{file_output}.\n")
    except FileNotFoundError:  # File not existed -> create file and add data
        with open(f'data/{file_output}', 'w+') as f_write:
            json.dump(new_data, f_write, indent=4)

        print(f"\tfile empty, create and add  {len(new_data)} data in data\\{file_output}.\n")
    return True
