from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from tools import crawler
import logging
from airflow.utils.dates import days_ago
from data.mongodb import ShopeeCrawlerDB
import os
import yaml
from datetime import timedelta
from random import randint
import concurrent.futures

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DEFAULT_ARGS = {
    'owner': 'lth',
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    'trigger_rule': "one_success",
    'wait_for_downstream': False
    }

# [START dag_decorator_usage]
@dag(default_args=DEFAULT_ARGS, schedule_interval=None, start_date=days_ago(0), tags=['datapipeline-etl_1'], concurrency=randint(5, 10), default_view='graph') # 
def airflow_etl():
    DATALAKE = ShopeeCrawlerDB(role='read_and_write')

    @task(retries=3, retry_exponential_backoff=True, task_concurrency=3)
    def extract(link, page):
        return crawler.crawl(link=link, newest=page*100)
    
    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def transform(extracted_data):
        return crawler.select_properties(extracted_data)

    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def load(transformed_data):
        return DATALAKE.insert_many_products(transformed_data)

    def etl(link, page):
        extracted_data = extract(link, page)
        transformed_data = transform(extracted_data)
        load(transformed_data)

    with open(f"{os.getcwd()}/dags/config/config.yml") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        links = data['links']
        pages = data['pages']

    
    for link in links:
        for page in range(0, pages, 1):
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                _id = f"{page}-{link.split('.')[-1]}"
                with TaskGroup(_id, tooltip="Tasks for section"):
                    futures.append(executor.submit(etl, link, page))

    # a link -> a job
    # a job contains 3 tasks(steps): extract -> transform -> load

dag = airflow_etl()
# [END dag_decorator_usage]