# ShopeeCrawler
Crawl data from Shopee and analyze 

## Instructions
Using python==3.8.5

1. Run ```pip install requirements.txt``` to install all needed packages
2. Crawl data
    2.1 Run ```main.py crawl_to_file --input config.yml --output <file_name>.json``` to start crawling and save to file
    2.2 Run ```main.py crawl --input config.yml``` to start crawling and insert to database
3. Run ```main.py visualize --file <file_name>.json <number_of_page>``` to visualize it


## Data visualization
- Data we'll be stored in _data/data.json_ as dicts in a list:

|product_id | shop_id  | product_name |   ...  |
|    :---:  |   :---:  |     :---:    |  :---: |
|1357623842 | 80094231 | product_name | others |