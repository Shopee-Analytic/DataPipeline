import json
import re

def visualize():
    with open('data/data.json') as f:
        data = json.load(f)

    for i in range(int(input("Enter number of product: "))):
        item = data[i]

        for prop in item:
            if prop != "category_ids":
                print(f"{prop}: {item[prop]}")
        print('\n')

if __name__ == "__main__":
    visualize()