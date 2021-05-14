from airflow.decorators import dag, task
from tools import crawler
import logging
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
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
@dag(default_args=DEFAULT_ARGS, schedule_interval=None, start_date=days_ago(0), tags=['datapipeline-etl_1'], concurrency=randint(3, 5), default_view='graph') # 
def airflow_etl_2():
    DATALAKE = ShopeeCrawlerDB(role='read_and_write')


    def extract(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push('extracted_data', crawler.crawl(kwargs['link'], kwargs['page']))

    def transform(**kwargs):
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(task_ids=f'extractor-{kwargs["_id"]}', key='extracted_data')
        ti.xcom_push('transformed_data', crawler.select_properties(extracted_data))

    def load(**kwargs):
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(task_ids=f'transform-{kwargs["_id"]}', key='transformed_data')
        DATALAKE.insert_many_products(transformed_data)

    with open(f"{os.getcwd()}/dags/config/config.yml") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        links = data['links']
        pages = data['pages']

    def etl(link, page):
        _id = f"{page}-{link.split('.')[-1]}"
        extract_task = PythonOperator(
            task_id=f"extractor-{_id}",
            python_callable=extract,
            op_kwargs={"_id": _id,"link": link, "page": page*100},
            retries=3
        )
        transform_task = PythonOperator(
            task_id=f"transformer-{_id}",
            python_callable=transform,
            op_kwargs={"_id": _id}
        )
        load_task = PythonOperator(
            task_id=f"loader-{_id}",
            python_callable=load,
            op_kwargs={"_id": _id}
        )

        extract_task >> transform_task >> load_task
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        for link in links:
            for page in range(0, pages, 1):
                executor.submit(etl, link, page)
    



    # a link -> a job
    # a job contains 3 tasks(steps): extract -> transform -> load

dag = airflow_etl_2()
# [END dag_decorator_usage]