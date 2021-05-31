from operator import index
from shutil import ExecError
import yaml
from psycopg2.pool import ThreadedConnectionPool
from random import uniform
import time
from contextlib import contextmanager
import os


def config(file_name="db-config.yml", section="postgre", role="admin") -> dict:
    path = f"{os.getcwd()}/dags/config"
    with open(f"{path}/{file_name}") as f:
        params = yaml.load(f, Loader=yaml.FullLoader)

    return params[section][role]

def retry_getpool_with_backoff(retries=4, backoff_in_seconds=1):
    def rwb(get_pool):
        def wrapper(role):
            x = 0
            while True:
                try:
                    return get_pool(role)
                except:
                    if x == retries:
                        raise
                    else:
                        sleep = (backoff_in_seconds * 2 ** x +
                                 uniform(0, 1))
                        time.sleep(sleep)
                        x += 1
        return wrapper
    return rwb

@retry_getpool_with_backoff()
def get_pool(role):
    params = config(role=role)
    return ThreadedConnectionPool(
        minconn = 1,
        maxconn = 5,
        host = params['host'],
        database = params['database'],
        user=params['user'],
        password=params['password']
    )

class DataWareHouse:
    def __init__(self, role='admin'):
        self.VERSION = "v7"
        self.role = role
        self.open_pool()
    def open_pool(self):
        try:
            self.pool = get_pool(self.role)
            return True
        except Exception as e:
            print(e)

    @contextmanager
    def get_cursor(self):
        con = self.pool.getconn()
        con.set_session(autocommit=True)
        try:
            yield con.cursor()
        finally:
            self.pool.putconn(con)
    
    def exec(self, command_file):
        try:
            with self.get_cursor() as cur:
                with open(command_file) as f:
                    for sql in f.read().split(";"):
                        if sql != "":
                            try:
                                cur.execute(sql)
                            except Exception as e:
                                print(e)
        except Exception as e:
            print(e)

    def check_connection(self):
        with self.get_cursor() as cur:
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')
            db_version = cur.fetchone()
            print(db_version)
    
    def copy_data_by_csv(self, file_path: str, table_name: str, keys: list, delimiter: str):
        try:
            with self.get_cursor() as cur:
                try:
                    with open(file_path, encoding="utf8") as f:
                        next(f)
                        cur.copy_from(f, self.VERSION+"."+table_name, columns=keys, sep=delimiter)
                except Exception as e:
                    raise e
            return True
        except Exception as e:
            raise e
    
    def save_to_csv(self, output_filename: str, command: str):
        try:
            outputquery = "COPY ({0}) TO STDOUT WITH CSV HEADER DELIMITER ';'".format(command)
            with self.get_cursor() as cur:
                file_path = f'{os.getcwd()}/{output_filename}.csv'
                with open(file_path, 'w', encoding='utf-8') as f:
                    cur.copy_expert(outputquery, f)
            return f"File successfully saved at: '{file_path}'"
        except Exception as e:
            print(e)
            return None
    
    def create_view(self):
        
        def create(view_name, command):
            drop = f"DROP VIEW IF EXISTS {self.VERSION}.{view_name}"
            sql = "CREATE VIEW {}.{} AS {}".format(self.VERSION, view_name, command)
            try:
                with self.get_cursor() as cur:
                    cur.execute(drop)
                    cur.execute(sql)
                    print(f"Create view {view_name} successfully!")
            except Exception as e:
                print(e)
                print(f"Failed to create view {view_name}!")

        arr = [
            {"view_name":  "tableView",
            "command": f"""
                select distinct on (product_id) product_name, product_image, product_link, product_brand, rating_star, rating_count, category_id, product_price, product_discount, product_price*(1-product_discount/100) as price_after_discount, currency, stock, sold, day, month, year
                from {self.VERSION}.product
                natural join {self.VERSION}.product_time
                natural join {self.VERSION}.product_brand
                natural join {self.VERSION}.product_rating
                natural join {self.VERSION}.product_price
                natural join {self.VERSION}.product_quantity
                order by product_id asc, datetime desc, product_discount desc
            """},
            {"view_name": "productPriceView",
            "command": f"""
                SELECT * FROM 
            """
            }
        ]

        for e in arr:
            create(e['view_name'], e['command'])


    def create_index(self):
        def create(index_name, table_name, columns):
            drop = f"DROP INDEX IF EXISTS {self.VERSION}.{index_name};"
            sql = f"CREATE INDEX {index_name} ON {self.VERSION}.{table_name} ({', '.join(map(str, columns))});"
            try:
                with self.get_cursor() as cur:
                    cur.execute(drop)
                    cur.execute(sql)
                    print(f"Create index {index_name} on {table_name} successfully!")
            except Exception as e:
                print(e)    
                print(f"Failed to create index {index_name} on {table_name}!")
        
        create(index_name="product_id_index", table_name="product", columns=['product_id', 'fetched_time'])
        create(index_name="product_time_index", table_name="product_time", columns=['day', 'month'])
if __name__ == "__main__":
    path = "dags\data\command\create_table.sql"
    DataWareHouse(role='admin').exec(path)
    DW = DataWareHouse()
    # DW.create_index()
    # DW.create_view()
    