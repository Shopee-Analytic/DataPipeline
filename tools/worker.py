from controller.mongodb import ShopeeCrawlerDB
from tools.crawler import select_properties, get_category_id, crawl, get_url
import time
import concurrent.futures
import logging
from datetime import datetime

count_total = count = 0
database = ShopeeCrawlerDB()


def start_crawling(urls_of_category):
    logging.basicConfig(filename='log/worker.log', level=logging.INFO)
    num_of_page = 80
    global count, count_total
    threaded_start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        try:
            futures = []
            for url_of_category in urls_of_category:
                logging.info(f"Url: {url_of_category}")
                logging.info("Started time: {}".format(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
                for newest in range(0, (num_of_page-1)*100 + 1, 100):
                    # print(f"Crawling {url_of_category} page {int(newest/100) + 1}!")r
                    futures.append(executor.submit(crawl_and_insert, url_of_category, newest))
                for _ in concurrent.futures.as_completed(futures):
                    # print(future.result())
                    if count != 0:
                        logging.info(f'Crawl {count} data from {url_of_category}')
                    count = 0
        except:
            logging.error("Stop crawling!!!!")
            exit()
        logging.info("Threaded time: {}".format(str(time.time() - threaded_start)))
        print(f"\n\nTotal data: {count_total}")
        logging.info(f"Total data: {count_total}")
        count_total = 0

def crawl_and_insert(url_of_category, newest):
    global count, count_total
    data = crawl(url_of_category, newest)
    count += data[1]
    count_total += data[1]
    database.insert_many_products(data[0])
