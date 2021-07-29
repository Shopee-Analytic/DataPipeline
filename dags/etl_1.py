from airflow.decorators import dag, task, task_group
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
@dag(default_args=DEFAULT_ARGS, tags=['datapipeline'], start_date=days_ago(1), schedule_interval=None, concurrency=6, max_active_runs=2, default_view='graph')
def etl_1():

    @task(retries=3, retry_exponential_backoff=True)
    def extract(link, page):
        return worker1.extract(link, page*100)
    
    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def transform(extracted_data):
        return worker1.transform(extracted_data)

    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def load(transformed_data):
        return worker1.load(transformed_data)

    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def index():
        return worker1.indexing()

    @task_group
    def etl(link, page):
        return load(transform(extract(link=link, page=page)))


    # etl(get_data_task.output) >> index()
    with open(f'{os.getcwd()}/dags/config/config-with-airflow.yml') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    etls = []
    for link in data['links']:
        for page in range(data['pages']):
            etls.append(etl(link, page))
    
    etls >> index()


dag1 = etl_1()
# [END dag_decorator_usage]
