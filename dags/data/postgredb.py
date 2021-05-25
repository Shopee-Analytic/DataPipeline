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
        self.VERSION = "v6"
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


if __name__ == "__main__":
    path = "dags\data\command\create_table.sql"
    DataWareHouse(role='admin').exec(path)