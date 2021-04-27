from apscheduler.schedulers.background import BackgroundScheduler, BlockingScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import time
from datetime import datetime, timedelta
import os
from pymongo import MongoClient
import logging
import logging.config
import json
from tools.worker import crawl_and_insert


# Load logger's config
logging.config.fileConfig('config/logging.conf')
# Create logger
logger = logging.getLogger('scheduler')

def get_scheduler():
    with open("controller/accounts.json") as f:
        data = json.load(f)
        admin = data['mongo']['admin']
    client = MongoClient(admin['server_link'])
    jobstores = {"mongo": MongoDBJobStore(client=client, collection="jobs")}
    executors = {'default': ThreadPoolExecutor(max_workers=20)}
    job_defaults = {"coalesce": False, "max_instances": 20, 'misfire_grace_time': None}

    return BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, jobstore="mongo")
    

def add_job(urls, trigger='interval', days=1, hours=0, minutes=00):
    pages = 81
    scheduler = get_scheduler()
    
    total = 0
    for url in urls:
        count = 0
        for newest in range(0, (pages)*100, 100):
            
            name=f"{int(newest/100)+1}-{url.split('.')[-1]}"
            
            scheduler.add_job(
                func=crawl_and_insert,
                args=(url, newest),
                trigger=trigger,
                days=days,
                hours=hours,
                minutes=minutes,
                name=name,
                id=name,
                start_date=(datetime.now()+timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:%S"),
                jobstore="mongo",
                replace_existing=True
            )
            count +=1
            logger.info(f'Create a new job "{name}".')
        logger.info(f"Added {count} jobs from {url} to jobstore.")
        total += count
    logger.info(f"Added {total} jobs from all urls to jobstore.")
    scheduler.start()
    while True:
        try:
            time.sleep(1)
        except (Exception, KeyboardInterrupt):
            scheduler.shutdown()
            exit()
    

def run():
    scheduler = get_scheduler()
    scheduler.start()
    while True:
        try:
            time.sleep(1)
        except (Exception, KeyboardInterrupt):
            scheduler.shutdown(wait=False)
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
