from airflow.decorators import dag, task
from airflow.models import DagRun
from airflow.utils.dates import days_ago

from tools import worker2

from datetime import timedelta, datetime
from random import randint
import os

if not os.path.exists(os.getcwd()+ '/dags/data/csv'):
    os.mkdir(os.getcwd()+ '/dags/data/csv')

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DEFAULT_ARGS = {
    'owner': 'lth',
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    'trigger_rule': 'one_success',
    'wait_for_downstream': False
    }

# [START dag_decorator_usage]
@dag(default_args=DEFAULT_ARGS, schedule_interval=None, start_date=days_ago(0), tags=['datapipeline'], concurrency=randint(5, 7), default_view='graph') # 
def etl_2():
    @task(retries=3, retry_exponential_backoff=True)
    def extract_shop(last_run):
        return worker2.extract_distinct_shop(last_run)
    
    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def extract_product(shop_id, last_run):
        return worker2.extract_product_from_shop(shop_id, last_run)

    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def transform(extracted_product):
        return worker2.transform(extracted_product=extracted_product)

    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def load(transformed_data):
        return worker2.load(transformed_data)

    def etl(last_run):
        extracted_shop_ids = extract_shop(last_run=last_run)

        for shop_id in extracted_shop_ids:
            extracted_product = extract_product(shop_id, last_run)
            transformed_data = transform(extracted_product=extracted_product)
            load(transformed_data=transformed_data)

    def last_time_run(dag_id='etl_1'):
        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        return float(datetime.timestamp(dag_runs[0].execution_date))

    
    etl(last_time_run())

    # a link -> a job
    # a job contains 3 tasks(steps): extract -> transform -> load

dag = etl_2()
# [END dag_decorator_usage]