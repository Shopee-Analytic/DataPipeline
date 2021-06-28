from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

from tools import worker1

import os
import yaml
from datetime import timedelta


import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


DEFAULT_ARGS = {
    'owner': 'lth',
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    'trigger_rule': 'one_success',
    'wait_for_downstream': False,
}
#"9 0 * * *"
# [START dag_decorator_usage]
@dag(default_args=DEFAULT_ARGS, tags=['datapipeline'], start_date=days_ago(1), schedule_interval=None, concurrency=3, max_active_runs=2, default_view='graph')
def etl_1():

    @task(retries=3, retry_exponential_backoff=True)
    def extract(link, newest):
        return worker1.extract(link=link, newest=newest)
    
    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def transform(extracted_data):
        return worker1.transform(extracted_data)

    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def load(transformed_data):
        return worker1.load(transformed_data)
    

    def etl(link: str, page: int):
        extracted_data = extract(link, newest=page*100)
        transformed_data = transform(extracted_data)
        load(transformed_data)

    @task(retries=3, retry_exponential_backoff=True)
    def indexing():
        return worker1.indexing()

    with open(f'{os.getcwd()}/dags/config/config-with-airflow.yml') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        links = data['links']
        pages = data['pages']

    for link in links:
        for page in range(0, pages, 1):
            _id = f'{page}-{link.split(".")[-1]}'
            with TaskGroup(_id, tooltip='Tasks for section'):
                etl(link, page)
    
    # indexing()
    # a link -> a job
    # a job contains 3 tasks(steps): extract -> transform -> load

dag = etl_1()
# [END dag_decorator_usage]