from queue import Queue
from threading import Thread
from tools.crawler import crawl
from  controller.mongodb import ShopeeCrawlerDB
import time


class Worker(Thread):
    def __init__(self, threadID, url_of_category, newest):
        Thread.__init__(self)
        self.threadID = threadID
        self.url_of_category = url_of_category
        self.database = ShopeeCrawlerDB()
        self.database.drop_product()
        self.newest = newest

    def run(self):
        print(f"Starting worker {self.threadID}!")
        data = crawl(url_of_category=self.url_of_category, newest=self.newest)
        print(f"Worker {self.threadID} is handling page {int(self.newest/100) + 1} of link {self.url_of_category}.")
        self.database.insert_many_products(data)
        # if not self.is_alive():
        print(f"Exiting worker {self.threadID}")

def start_crawling(urls, num_of_page):
    
    for url in urls:
        print(f"Crawling {url}:") # 2 workers working with the same link

        newest_list = [i for i in range(0, (num_of_page-1)*100+1, 100)]
        while len(newest_list) >0:
            # Create and asign task for workers
            thread1 = Worker(1, url, newest_list.pop())
            thread1.start()
            if len(newest_list) > 0:
                thread2 = Worker(2, url, newest_list.pop())
                thread2.start()
                
            
    # Wait until both workers done.
    thread1.join()
    try:
        thread2.join()
    except UnboundLocalError:
        print("List is empty, worker 2 have nothing to do.")
            


