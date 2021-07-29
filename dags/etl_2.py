from airflow.decorators import dag, task, task_group
from airflow.models import DagRun
from airflow.utils.dates import days_ago
from tools import worker2
from datetime import timedelta, datetime
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
    'wait_for_downstream': False,
}
# "18 0 * * *"
# [START dag_decorator_usage]
@dag(default_args=DEFAULT_ARGS, tags=['datapipeline'], start_date=days_ago(1), schedule_interval=None, concurrency=8, max_active_runs=2, default_view='graph')
def etl_2():
    def get_last_run(dag_id='etl_1'):
        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        return datetime.timestamp(dag_runs[0].execution_date) - 7*60*60 if dag_runs else None

    def extract_shops(last_run):
        return worker2.extract_distinct_shop(last_run)

    @task(retries=3, retry_exponential_backoff=True)
    def extract(last_run, extracted_shops):
        return worker2.extract_product_from_shops(extracted_shops, last_run)

    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def transform(extracted_product, sub_name):
        return worker2.transform(extracted_product=extracted_product, sub_name=sub_name)

    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def load(transformed_data):
        return worker2.load(transformed_data)
    
    @task(depends_on_past=True)
    def index():
        return worker2.create_view_and_index()

    @task_group
    def etl(last_run, extracted_shops, sub_name):
        return load(transform(extract(last_run, extracted_shops), sub_name=sub_name))


    last_run = get_last_run()
    extracted_shops = extract_shops(last_run)


    etls = []
    for i in range(0, len(extracted_shops), 50):
        etls.append(etl(last_run, extracted_shops[i:i+50], i))

    etls >> index()


dag2 = etl_2()
# [END dag_decorator_usage]