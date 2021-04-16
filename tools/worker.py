from controller.mongodb import ShopeeCrawlerDB
from tools.crawler import select_properties, get_category_id, crawl, get_url
import time
import concurrent.futures

count_total = count = 0


def start_crawling(urls_of_category, num_of_page):
    threaded_start = time.time()
    global count, count_total
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for url_of_category in urls_of_category:
            for newest in range(0, (num_of_page-1)*100 + 1, 100):
                # print(f"Crawling {url_of_category} page {int(newest/100) + 1}!")
                futures.append(executor.submit(
                    crawl_and_insert, url_of_category, newest))
            for future in concurrent.futures.as_completed(futures):
                # print(future.result())
                if count != 0:
                    print(f'Crawl {count} data from {url_of_category}')
                count = 0
            print()
    print("Threaded time:", time.time() - threaded_start)
    print(f"\n\nTotal data: {count_total}")


database = ShopeeCrawlerDB()


def crawl_and_insert(url_of_category, newest):
    global count, count_total
    data = crawl(url_of_category, newest)
    count += data[1]
    count_total += data[1]
    database.insert_many_products(data[0])
