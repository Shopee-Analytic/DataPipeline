import os
from data.mongodb import ShopeeCrawlerDB
import logging
from tools.crawler import crawl
import concurrent.futures
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


datalake = ShopeeCrawlerDB()

def crawl_and_insert(link, newest):
    try:
        data = crawl(link, newest)
        count = len(datalake.insert_many_products(data))
        logger.info(f"Successfully crawled and inserted {count} data from {newest}-{link.split('.')[-1]}.")
    except Exception as e:
        logger.error(e)
        return 0
    else:
        return count

if __name__ == "__main__":
    crawl_and_insert()