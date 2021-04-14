# ShopeeCrawler
Crawl data from Shopee and analyze 

## Instructions
Using python==3.8.5

1. Run ```pip install requirements.txt``` to install all needed packages
2. Run ```main.py crawl --input config.yml --output data.json``` to start crawling
3. Run ```main.py visualize --file data.json <number_of_page>``` to visualize it


## Data visualization
- Data we'll be stored in _data/data.json_ as dicts in a list:

|product_id | shop_id  | product_name |   ...  |
|    :---:  |   :---:  |     :---:    |  :---: |
|1357623842 | 80094231 | product_name | others |