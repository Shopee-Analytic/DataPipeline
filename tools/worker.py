from controller.mongodb import ShopeeCrawlerDB
from tools.crawler import select_properties, get_category_id, crawl, get_url
import time
import concurrent.futures
import logging
from datetime import datetime

database = ShopeeCrawlerDB()

def start_crawling(urls_of_category):
    logging.basicConfig(filename='log/worker.log', level=logging.INFO)
    
    count_total = 0
    threaded_start = time.time()

    with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
        try:
            futures = []
            for url_of_category in urls_of_category:
                logging.info('Start crawling "{}" at {}'.format(url_of_category ,datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
                futures.append(executor.submit(handling_crawler, url_of_category))
                time.sleep(2)
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                count = future.result()
                if count == 0:
                    logging.info("There is nothing new from {}".format(urls_of_category[i]))
                else:
                    count_total += count
                    logging.info('Crawl {} new data from {}'.format(count, urls_of_category[i]))
        except Exception as e:
            print(e)
            logging.error("Stop crawling!!!!")
            executor.shutdown()
            exit()
        logging.info("Threaded time: {}".format(str(time.time() - threaded_start)))
        print(f"\n\nTotal data: {count_total}")
        logging.info(f"Total data: {count_total}")

def handling_crawler(url_of_category):
    num_of_page = 80
    count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        try:
            futures = []
            for newest in range(0, (num_of_page-1)*100 + 1, 100):
                futures.append(executor.submit(crawl_and_insert, url_of_category, newest))
        except:
            exit()
        else:
            for future in concurrent.futures.as_completed(futures):
                count += future.result()
            for future in futures:
                while not future.done():
                    pass
        return count

def crawl_and_insert(url_of_category, newest):
    count = 0
    data = crawl(url_of_category, newest)
    count += len(database.insert_many_products(data))
    return count