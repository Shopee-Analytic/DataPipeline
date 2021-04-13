import requests
import json
import re

# Test if web response
# print(requests.get('https://shopee.vn'))

url_search = "https://shopee.vn/dien-thoai-phu-kien-cat.84"

# url_search = input("Enter link: ")

match = re.search(r'.+-cat.(\d+)', url_search)
# .+-cat.(\d+) SHOP ONLY
# .+-i.(\d+\.\d+) item_id and shop_id

# item_id = match.group(1)
shop_id = match.group(1)
url = 'https://shopee.vn/api/v4/search/search_items?by=relevancy&limit=100&match_id={}&newest=0&order=desc&page_type=search&version=2'

r = requests.get(url.format(shop_id), headers={"content-type":"text"})


data = r.json()
# print(data)
with open("data.json", 'w+') as f:
    json.dump(data['items'], f, indent=4)