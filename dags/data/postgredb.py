from sys import version
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
        maxconn = 125,
        host = params['host'],
        database = params['database'],
        user=params['user'],
        password=params['password'],
        port=params['port']
    )

class DataWareHouse:
    def __init__(self, role='admin'):
        self.VERSION = "t01"
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
            if view_name and command:
                drop = f"DROP MATERIALIZED VIEW IF EXISTS {self.VERSION}.{view_name}"
                sql = "CREATE MATERIALIZED VIEW {}.{} AS {}".format(self.VERSION, view_name, command)
                refresh = f"REFRESH MATERIALIZED VIEW {self.VERSION}.{view_name}"
                try:
                    with self.get_cursor() as cur:
                        cur.execute(drop)
                        cur.execute(sql)
                        cur.execute(refresh)
                        print(f"Create view {view_name} successfully!")
                except Exception as e:
                    print(e)
                    print(f"Failed to create view {view_name}!")
            else:
                print(f'failed to create view {view_name}')


        def create_command(distinct: list=[], columns: list=[], main_table: str="", join_tables: list=[], orders: dict={}):
            command = "select "
            if len(distinct) > 0:
                command += f"distinct on ({', '.join(map(str, distinct))})"

            if columns:
                command += f" {', '.join(map(str, columns))}\n"
                
            command += f"from {self.VERSION}.{main_table}\n"

            if join_tables:
                command += f'natural join {self.VERSION}.'+'\nnatural join {}.'.format(self.VERSION).join(map(str, join_tables)) + "\n"

            if len(orders) > 0:
                command += "order by "
                for i, (key, value) in enumerate(orders.items()):
                    command += f"{key} {value}"
                    command += ", " if i != len(orders)-1 else " "

            return command
 
        arr = [
            {"view_name":  "tableView",
            "command": create_command(
                distinct=['product_id', 'day', 'month', 'year'],
                columns=['product_id', 'product_name', 'product_image', "product_link", "rating_star", "category_id", 'product_price', "product_discount", "currency", "stock", "sold", "day", "month", "year"],
                main_table= "product",
                join_tables=["product_time", "product_brand", "product_rating", "product_price", "product_quantity"],
                orders= {"year": "desc", "month": "desc", "day": "desc"}
                )
            },
            {"view_name": "productView",
            "command": create_command(
                distinct=['product_id', 'day', 'month', 'year'],
                columns=['product_id', 'product_name', 'product_image', "product_link", "rating_star", "rating_count", "label_ids", "category_id", 'product_price', "product_discount", '(product_price*(1-product_discount/100)) as price_after_discount', "currency", "stock", "sold", "day", "month", "year"],
                main_table="product",
                join_tables=["product_time", "product_brand", "product_rating", "product_price", "product_quantity"],
                orders= {"year": "desc", "month": "desc", "day": "desc"}
            )}
        ]
        
        for i in arr:
            create(view_name = i["view_name"], command = i['command'])


    def create_index(self):
        def create(index_name, table_name, columns):
            # drop = f"DROP INDEX IF EXISTS {self.VERSION}.{index_name};"
            sql = f"CREATE UNIQUE INDEX {index_name} ON {self.VERSION}.{table_name} ({', '.join(map(str, columns))});"
            try:
                with self.get_cursor() as cur:
                    # cur.execute(drop)
                    cur.execute(sql)
                    print(f"Create index {index_name} on {table_name} successfully!")
            except Exception as e:
                print(e)    
                print(f"Failed to create index {index_name} on {table_name}!")
        
        arr = [
            {"index_name": "productView_index",
            "table_name": "productView",
            "columns": ["day", "month", "year", "product_id"]
            },
            {"index_name": "tableView_index",
            "table_name": "tableView",
            "columns": ["day", "month", "year", "product_id"]
            },            
        ]

        for i in arr:
            create(i['index_name'], i['table_name'], i["columns"])

if __name__ == "__main__":
    # path = "dags\data\command\create_table.sql"
    # DataWareHouse(role='admin').exec(path)
    DW = DataWareHouse()
    DW.create_view()
    DW.create_index()
    