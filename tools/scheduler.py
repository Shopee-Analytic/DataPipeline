from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import time
from datetime import datetime, timedelta
import os
from pymongo import MongoClient
import logging
import logging.config
import json

with open("controller/accounts.json") as f:
    data = json.load(f)    
    admin = data['admin']

logging.config.fileConfig('config/logging.conf')

# create logger
logger = logging.getLogger('scheduler')

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
    
    scheduler = get_scheduler()
    scheduler.add_job(crawl, trigger=trigger, minutes=minutes, jobstore="mongo", name=name, jitter=30, start_date=(datetime.now()+timedelta(seconds=10)).strftime("%Y-%m-%d %H:%M:%S"))
    logger.info('Create a new job "{}".'.format(name))
    scheduler.start()
    scheduler.print_jobs(jobstore="mongo")
    scheduler.shutdown()

def run():

    scheduler = get_scheduler()
    scheduler.start()
    scheduler.print_jobs(jobstore="mongo")
    if len(scheduler.get_jobs()) <=0:
        print("There is no job in schedule")
    else:
        logger.info("Start crawling")
        while True:
            try:
                time.sleep(1)
            except (KeyboardInterrupt, Exception):
                scheduler.shutdown()
                print("Stop working!")
                logger.error("Stoping Scheduler!")
                logger.info("Ended")
                exit()

def remove_all():
    scheduler = get_scheduler()
    scheduler.start()
    scheduler.remove_all_jobs(jobstore="mongo")
    scheduler.print_jobs()
    scheduler.shutdown()
    logger.info("Remove all jobs")

def show_all():
    scheduler = get_scheduler()
    scheduler.start()
    scheduler.print_jobs()
    scheduler.shutdown()
