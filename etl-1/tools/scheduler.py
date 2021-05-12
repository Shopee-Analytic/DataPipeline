from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
import time
from datetime import datetime
import logging
from data.mongodb import get_client
from tools.worker import crawl_and_insert
import yaml


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_scheduler(scheduler_type = "background"):
    client = get_client("read_and_write")

    jobstores = {"mongo": MongoDBJobStore(client=client, collection="jobs")}
    executors = {'default': ThreadPoolExecutor(max_workers=20)}
    job_defaults = {"coalesce": False, "max_instances": 20, 'misfire_grace_time': None}
    
    return BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults, jobstore="mongo") # jobstore = "default" for store in memories

def add_job(file="config.yml"):
    scheduler = get_scheduler()
    
    path = "config"
    with open(f'{path}/{file}') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        links = data['links']
        number_of_pages = data['number_of_pages']
    
    logger.info("Start adding jobs")
    for link in links:
        for newest in range(0, (number_of_pages-1)*100+1, 100):
            name = f"{int(newest/100)+1}-{link.split('.')[-1]}"
            scheduler.add_job(
                func=crawl_and_insert,
                args=(link, newest),
                trigger='interval',
                hours=6,
                name=name,
                id=name,
                start_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                jobstore="mongo",
                replace_existing=True
            )
            logging.info(f'Create a new job "{name}".')
    scheduler.start()
    scheduler.print_jobs()

def run_job(now=False, _id=None):
    scheduler = get_scheduler()
    scheduler.start()
    if _id is None:
        if now:
            for job in scheduler.get_jobs(jobstore="mongo"):
                job.modify(next_run_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        if now:
            job = scheduler.get_jobs(jobstore="mongo", _id=_id)
            job.modify(next_run_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    while True:
        try:
            time.sleep(1)
        except (KeyboardInterrupt, Exception):
            scheduler.shutdown()
            exit()

def remove_job(job_id=None):
    scheduler = get_scheduler()
    scheduler.start()
    if job_id is not None:
        scheduler.remove_job(job_id)
    else:
        scheduler.remove_all_jobs()
    scheduler.print_jobs()
if __name__ == "__main__":
    add_job()
    run_job()
