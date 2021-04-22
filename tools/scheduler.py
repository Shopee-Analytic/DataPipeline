from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import time
from datetime import datetime, timedelta
import os
from pymongo import MongoClient
import logging
import json

with open("controller/accounts.json") as f:
    data = json.load(f)    
    admin = data['admin']

def crawl():
    print("Start a job!")
    os.system("python main.py crawl --input config.yml")

def get_scheduler(jobstore="mongo"):
    client = MongoClient("mongodb+srv://{}:{}@cluster0.b2b5a.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE".format(admin["username"], admin["password"]))
    jobstores = {
        "mongo": MongoDBJobStore(client=client)
    }
    executors = {
        'default': ThreadPoolExecutor(max_workers=20),
        'processpool': ProcessPoolExecutor(max_workers=5)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 3
    }
    return BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, jobstore='mongo')



def add_job(trigger='interval', minutes=30, name="Crawl data from shopee"):
    logging.basicConfig(filename="log/scheduler.log", level=logging.INFO)
    
    scheduler = get_scheduler()
    scheduler.add_job(crawl, trigger=trigger, minutes=minutes, jobstore="mongo", name=name, jitter=30, start_date=(datetime.now()+timedelta(seconds=10)).strftime("%Y-%m-%d %H:%M:%S"))
    logging.info("Create a new job {} at {}.".format(name, (datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))))
    scheduler.start()
    scheduler.print_jobs(jobstore="mongo")

def run():
    logging.basicConfig(filename="log/scheduler.log", level=logging.DEBUG)

    scheduler = get_scheduler()
    scheduler.start()
    scheduler.print_jobs(jobstore="mongo")
    if len(scheduler.get_jobs()) <=0:
        print("There is no job in schedule")
    else:
        logging.info("Start crawling at {}.".format(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
        while True:
            try:
                time.sleep(1)
            except (KeyboardInterrupt, Exception):
                scheduler.shutdown()
                print("Stop working!")
                logging.error("Stoping Scheduler!")
                logging.info("Ended time: {}".format(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))
                exit()

def remove_all():
    logging.basicConfig(filename="log/scheduler.log", level=logging.INFO)
    scheduler = get_scheduler()
    scheduler.start()
    scheduler.remove_all_jobs(jobstore="mongo")
    scheduler.print_jobs()
    scheduler.shutdown()
    logging.info("Remove all jobs at {}".format(datetime.now().strftime("%m/%d/%Y, %H:%M:%S")))

def show_all():
    scheduler = get_scheduler()
    scheduler.start()
    scheduler.print_jobs()
    scheduler.shutdown()
