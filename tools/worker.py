from controller.mongodb import ShopeeCrawlerDB
from tools.crawler import select_properties, get_category_id, crawl, get_url
import time
import concurrent.futures
import logging
import logging.config
from datetime import datetime

database = ShopeeCrawlerDB()

logging.config.fileConfig('config/logging.conf')

# create logger
logger = logging.getLogger('worker')

def start_crawling(urls_of_category):
    
    count_total = 0
    threaded_start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        try:
            futures = []
            for url_of_category in urls_of_category:
                logger.info('Start crawling "{}"'.format(url_of_category))
                time_start = time.time()
                futures.append(executor.submit(handling_crawler, url_of_category))
            for future in concurrent.futures.as_completed(futures):
                count = future.result()
                i = futures.index(future)
                logger.info(f'Done crawling "{url_of_category}" in {time.time() - time_start)}s.'))
                if count == 0:
                    logger.info("There is nothing new from {}".format(urls_of_category[i]))
                else:
                    count_total += count
                    logger.info('Crawl {} new data from {}'.format(count, urls_of_category[i]))
        except Exception as e:
            print(e)
            logger.error("Stop crawling!!!!")
            executor.shutdown()
            exit()
        logger.info("Threaded time: {}".format(str(time.time() - threaded_start)))
        logger.info(f"Total data: {count_total}")

def handling_crawler(url_of_category):
    num_of_page = 80
    count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        try:
            futures = []
            for newest in range(0, (num_of_page-1)*100 + 1, 100):
                futures.append(executor.submit(crawl_and_insert, url_of_category, newest))
        except Exception as e:
            print(e)
            logger.error(e)
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