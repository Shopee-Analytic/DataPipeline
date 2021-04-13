import json
import re
import pandas as pd
with open('data.json') as f:
    data = json.load(f)

for i in range(100):
    item = data[i]['item_basic']

    item_id = item['itemid']
    item_price = item['price']
    item_discount = item['raw_discount']
    shop_id = item['shopid']
    stock = item['stock']
    sold = item['sold']
    image = item['image']
    rating_star = item['item_rating']['rating_star']
    rating_count = item['item_rating']['rating_count']


    print(f"Item {item_id}: ")
    print(f"\tPrice: {item_price}")
    print(f"\tDiscount: {item_discount}")
    print(f'\tShopeID: {shop_id}')
    print(f"\tStock: {stock}")
    print(f"\tSold: {sold}")
    print('\n')
    
