from airflow.decorators import dag, task
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

# [START dag_decorator_usage]
@dag(default_args=DEFAULT_ARGS, tags=['datapipeline'], start_date=days_ago(1), schedule_interval="18 0 * * *", concurrency=3, max_active_runs=2, default_view='graph')
def etl_2():
    @task(retries=3, retry_exponential_backoff=True)
    def extract_shop(last_run):
        return worker2.extract_distinct_shop(last_run)
    
    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def extract_product(shop_ids, last_run):
        return worker2.extract_product_from_shops(shop_ids, last_run)

    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def transform(extracted_product, sub_name):
        return worker2.transform(extracted_product=extracted_product)

    @task(depends_on_past=True, retries=3, retry_exponential_backoff=True)
    def load(transformed_data):
        return worker2.load(transformed_data)
    
    @task(depends_on_past=True)
    def indexing(etl):
        return worker2.create_view_and_index()

    def etl(shop_ids, last_run, sub_name):
        extracted_product = extract_product(shop_ids, last_run)
        transformed_data = transform(extracted_product=extracted_product, sub_name=sub_name)
        loading = load(transformed_data=transformed_data)
        indexing(loading)

    def get_most_recent_dag_run(dag_id='etl_1'):
        dag_runs = DagRun.find(dag_id=dag_id)
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        return datetime.timestamp(dag_runs[0].execution_date) if dag_runs else None

    last_run = get_most_recent_dag_run()
    limit = 10
    if last_run:
        shop_ids = extract_shop(last_run=last_run)
        etl(shop_ids, last_run, "")
        # for i in range(0, len(shop_ids), limit):
        #     etl(shop_ids[i:i+limit], last_run, i)
    # worker2.create_view_and_index()

    

    # a link -> a job
    # a job contains 3 tasks(steps): extract -> transform -> load

dag2 = etl_2()
# [END dag_decorator_usage]