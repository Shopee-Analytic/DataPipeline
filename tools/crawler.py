import requests
import json
import re
from datetime import datetime

def crawl(url_of_category, file_output):

    # Test if web response
    if requests.get(url_of_category).status_code != 200:
        return False

    # Using regex to match the category_id
    category_id = re.search(r'https://shopee.vn/.+-cat.(\d+)', url_of_category)
    if not category_id:
        return False

    shop_id = category_id.group(1)
    newest=0 
    limit = 100 # newest = 0 -> `limit` newest product
    url = 'https://shopee.vn/api/v4/search/search_items?by=relevancy&limit={}&match_id={}&newest={}&order=desc&page_type=search&version=2'

    data = requests.get(url.format(limit, shop_id, newest),
                        headers={"content-type": "text"})

    return save_data_to_file(file_output, select_properties(data.json()))

def select_properties(new_data): # data = [{}, {}, {}, ...]
    data = []

    items = new_data["items"]
    for item in items:
        item = item["item_basic"]
        data.append( 
            {
                "product_id": item['itemid'],
                "shop_id": item["shopid"],
                "product_name": item["name"],
                "category_ids": item["label_ids"],
                "image": "https://cf.shopee.vn/file/{}_tn".format(item["image"]),
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
                "fetched_timestamp": datetime.timestamp(datetime.now())
            }
        )

    return data


def save_data_to_file(file_output, new_data): # data = [{}, {}, {}, ...]
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
    except FileNotFoundError: # File not existed -> create file and add data
        with open(f'data/{file_output}', 'w+') as f_write:
            json.dump(new_data, f_write, indent=4)
        
        print(f"\tfile empty, create and add  {len(new_data)} data in data\\{file_output}.\n")

    else:
        print("Another unknown error.")

    return True
