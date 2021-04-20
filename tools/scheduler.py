from apscheduler.schedulers.background import BlockingScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
import time
from datetime import datetime
import os
from pymongo import MongoClient

def crawl():
    print("Run crawler!")
    os.system("python main.py crawl --input config.yml")

if __name__ == "__main__":
    admin={"username": "admin","password": "olQc3qAr4GY3h8TB"}
    client = MongoClient("mongodb+srv://{}:{}@cluster0.b2b5a.mongodb.net/myFirstDatabase?retryWrites=true&w=majority&ssl=true&ssl_cert_reqs=CERT_NONE".format(admin["username"], admin["password"]))
    jobstores = {
        "mongo": MongoDBJobStore(client=client)
    }

    executors = {
        'default': ThreadPoolExecutor(10),
        'processpool': ProcessPoolExecutor(2)
    }
    job_defaults = {
        'coalesce': False,
        'max_instances': 3
    }

    scheduler = BlockingScheduler(jobstores=jobstores, executors = executors, job_defaults = job_defaults)

    scheduler.add_job(crawl)
    scheduler.add_job(crawl, 'interval', minutes=15)

    
    scheduler.start()

