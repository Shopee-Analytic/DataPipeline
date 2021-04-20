from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import time
from datetime import datetime
import os
from pymongo import MongoClient
import logging


def crawl():
    print("Start a job!")
    os.system("python main.py crawl --input config.yml")

if __name__ == "__main__":

    logging.basicConfig(filename="log/scheduler.log", level=logging.INFO)
    logging.debug("Run crawler.")
    logging.info("Started time: {}".format(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
    admin={"username": "admin","password": "olQc3qAr4GY3h8TB"}
    client = MongoClient("mongodb+srv://{}:{}@cluster0.b2b5a.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE".format(admin["username"], admin["password"]))
    jobstores = {
        "mongo": MongoDBJobStore(client=client)
    }

    executors = {
        'default': ThreadPoolExecutor(max_workers=5),
        'processpool': ProcessPoolExecutor(max_workers=5)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 3
    }

    scheduler = BackgroundScheduler(jobstores=jobstores, executors = executors, job_defaults = job_defaults)

    # scheduler.add_job(crawl, jobstore="mongo", id="Instantly")
    # scheduler.add_job(crawl, 'interval', hours=1, jobstore="mongo", name="Crawling SHOPEE")

    scheduler.start()
    scheduler.print_jobs(jobstore="mongo")
    while True:
        try:
            time.sleep(1)
            
        except (KeyboardInterrupt, Exception):
            scheduler.shutdown()
            logging.error("Stoping Scheduler!")
            logging.info("Ended time: {}".format(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
            exit()
            
    