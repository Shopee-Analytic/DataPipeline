import json
import re
import pandas as pd
with open('data/data.json') as f:
    data = json.load(f)

for i in range(int(input("Enter number of product: "))):
    item = data[i]

    for prop in item:
        if prop != "category_ids":
            print(f"{prop}: {item[prop]}")
    print('\n')
