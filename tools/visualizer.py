import json
import re

def visualize(file, number_of_item):
    with open(f'data/{file}') as f:
        data = json.load(f)
    try:
        assert int(number_of_item) < len(data), f"Number of item is too large. It should be < {len(data)}"
        length = int(number_of_item)
    except AssertionError as msg:
        print(msg)
        return

    for i in range(length):
        item = data[i]

        for prop in item:
            if prop != "category_ids":
                print(f"{prop}: {item[prop]}")
        print('\n')
