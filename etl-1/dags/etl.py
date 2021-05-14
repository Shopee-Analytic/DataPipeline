from airflow.decorators import dag, task
from tools import crawler
import logging
from airflow.utils.dates import days_ago
from data.mongodb import ShopeeCrawlerDB
import os
import yaml
from random import randint


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DEFAULT_ARGS = {"owner": "lth"}


# [START dag_decorator_usage]
@dag(default_args=DEFAULT_ARGS, schedule_interval="@once", start_date=days_ago(2), tags=['datapipeline-etl_1'], max_active_runs=randint(5, 10))
def airflow_etl():
    DATALAKE = ShopeeCrawlerDB(role='read_and_write')
    @task(retries=3)
    def extract(link, newest):
        return crawler.crawl(link, newest)

    @task()
    def transform(extracted_data):
        return crawler.select_properties(extracted_data)

    @task(retries=3)
    def load(transformed_data):
        return DATALAKE.insert_many_products(transformed_data)

    with open(f"{os.getcwd()}/dags/config/config.yml") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        links = data['links']
        pages = data['pages']

    for link in links:
        for page in range(0, pages, 1):
            extracted_data = extract(link=link, newest=page*100)
            transformed_data = transform(extracted_data)
            load(transformed_data)

dag = airflow_etl()
# [END dag_decorator_usage]