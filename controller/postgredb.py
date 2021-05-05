from configparser import ConfigParser
import psycopg2
from psycopg2 import pool
from random import uniform
import time

def config(filename='test/database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, filename))

    return db

def retry_getpool_with_backoff(retries=4, backoff_in_seconds=1):
    def rwb(get_pool):
        def wrapper():
            x = 0
            while True:
                try:
                    return get_pool()
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
def get_pool():
    params = config()
    postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(1, 4, **params)
    
    return postgreSQL_pool

def get_connection(postgreSQL_pool):
    return postgreSQL_pool.getconn()

class DataWareHouse:
    pool = get_pool()
    
    db_version = "v3"

    def create_tables_v3(self):
        commands = (
            """
            DROP SCHEMA IF EXISTS v3 cascade
            """,
            """
            CREATE SCHEMA v3
            """,
            """
            SET search_path TO v3
            """,
            """
            DROP TABLE IF EXISTS
                shop_location,
                shop,
                product,
                product_shop,
                product_rating,
                product_time,
                currency,
                product_price,
                product_feedback,
                product_quantity
                CASCADE
            """,
            """ 
            CREATE TABLE shop_location (
                shop_location_id SERIAL PRIMARY KEY NOT NULL,
                shop_city VARCHAR(255) NOT NULL
            )
            """,
            """ 
            CREATE TABLE shop (
                shop_id BIGINT PRIMARY KEY,
                shop_location_id INTEGER NOT NULL,
                shopee_verified BOOL NOT NULL,
                FOREIGN KEY (shop_location_id) REFERENCES shop_location (shop_location_id) ON UPDATE CASCADE ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE product (
                product_id BIGINT PRIMARY KEY NOT NULL,
                product_name VARCHAR(255) NOT NULL,
                product_image VARCHAR(255) NOT NULL,
                product_link VARCHAR(255) NOT NULL
            )
            """,
            """
            CREATE TABLE product_shop (
                product_id BIGINT PRIMARY KEY NOT NULL,
                shop_id BIGINT NOT NULL,
                FOREIGN KEY (product_id) REFERENCES product (product_id) ON UPDATE CASCADE ON DELETE CASCADE,
                FOREIGN KEY (shop_id) REFERENCES shop (shop_id) ON UPDATE CASCADE ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE product_rating (
                product_id BIGINT PRIMARY KEY NOT NULL,
                rating_star REAL NOT NULL,
                rating_count BIGINT NOT NULL,
                FOREIGN KEY (product_id) REFERENCES product (product_id) ON UPDATE CASCADE ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE currency (
                currency_id SERIAL PRIMARY KEY NOT NULL,
                currency VARCHAR(255) NOT NULL
            )
            """,
            """
            CREATE TABLE product_price (
                product_id BIGINT PRIMARY KEY NOT NULL,
                price FLOAT8 NOT NULL,
                discount REAL NOT NULL,
                currency_id INTEGER NOT NULL,
                FOREIGN KEY (currency_id) REFERENCES currency (currency_id) ON UPDATE CASCADE ON DELETE CASCADE,
                FOREIGN KEY (product_id) REFERENCES product (product_id) ON UPDATE CASCADE ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE product_feedback (
                product_id BIGINT PRIMARY KEY NOT NULL,
                feedback_count INTEGER NOT NULL,
                FOREIGN KEY (product_id) REFERENCES product (product_id) ON UPDATE CASCADE ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE product_quantity (
                product_id BIGINT PRIMARY KEY NOT NULL,
                sold INTEGER NOT NULL,
                stock INTEGER NOT NULL,
                FOREIGN KEY (product_id) REFERENCES product (product_id) ON UPDATE CASCADE ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE product_time (
                product_id BIGINT PRIMARY KEY NOT NULL,
                fetched_time numeric NOT NULL,
                FOREIGN KEY (product_id) REFERENCES product (product_id) ON UPDATE CASCADE ON DELETE CASCADE
            )
            """,
            )
        try:
            conn = get_connection(self.pool)
            cur = conn.cursor()
            for command in commands:
                cur.execute(command)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                
    def insert_shop_location(self, shop_city):
        sql = [
            "SELECT shop_location_id FROM "+self.db_version+".shop_location WHERE shop_city=%s",
            "INSERT INTO "+self.db_version+".shop_location (shop_city) VALUES (%s) RETURNING shop_location_id"
        ]
        shop_location_id = None
        try:
            conn = get_connection(self.pool)
            cur = conn.cursor()
            try:
                cur.execute(sql[0], (shop_city, ))
                shop_location_id = cur.fetchone()[0]
            except TypeError:
                cur.execute(sql[1], (shop_city, ))
                shop_location_id = cur.fetchone()[0]
                print(f"Not found, insert '{shop_city}' to shop_location as index {shop_location_id}")
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
        else:
            self.pool.putconn(conn)
            return shop_location_id
        return None

    def insert_shop(self, shop_id, shop_city, shopee_verified):
        sql = "INSERT INTO "+ self.db_version + """.shop (shop_id,shop_location_id,shopee_verified)
                VALUES (%s,%s,%s)
                ON CONFLICT (shop_id)
                DO UPDATE SET shop_location_id = excluded.shop_location_id, shopee_verified = excluded.shopee_verified
                RETURNING shop_id"""
        shop_location_id = self.insert_shop_location(shop_city)
        try:
            conn = get_connection(self.pool)
            cur = conn.cursor()
            try:
                cur.execute(sql, (shop_id, shop_location_id, shopee_verified, ))
            except Exception as e:
                print(e)
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
        else:
            self.pool.putconn(conn)
            return shop_id
        return None

    def insert_product(self, product_id, product_name, product_image, product_link):
        sql = "INSERT INTO "+self.db_version+""".product (product_id, product_name, product_image, product_link)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET product_name = excluded.product_name, product_image = excluded.product_image, product_link = excluded.product_link
        RETURNING product_id
        """
        try:
            conn = get_connection(self.pool)
            cur = conn.cursor()
            cur.execute(sql, (product_id, product_name, product_image, product_link, ))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
        else:
            self.pool.putconn(conn)
            return product_id
        return None

    def insert_product_shop(self, product_id, shop_id):
        sql = "INSERT INTO "+self.db_version+""".product_shop (product_id, shop_id)
        VALUES (%s, %s)
        ON CONFLICT (product_id)
        DO NOTHING
        RETURNING product_id, shop_id
        """     
        try:
            conn = get_connection(self.pool)
            cur = conn.cursor()
            cur.execute(sql, (product_id, shop_id, ))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
        else:
            self.pool.putconn(conn)
            return product_id, shop_id
        return None

    def insert_currency(self, currency):
        sql = [
            "SELECT currency_id FROM "+self.db_version+".currency WHERE currency=%s",
            "INSERT INTO "+self.db_version+".currency (currency) VALUES (%s) RETURNING currency_id"
        ]
        currency_id = None
        try:
            conn = get_connection(self.pool)
            cur = conn.cursor()
            try:
                cur.execute(sql[0], (currency, ))
                currency_id = cur.fetchone()[0]
            except TypeError:
                cur.execute(sql[1], (currency, ))
                currency_id = cur.fetchone()[0]
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
        else:
            self.pool.putconn(conn)
            return currency_id
        return None

    def insert_product_price(self, product_id, price, discount, currency):
        sql = "INSERT INTO "+self.db_version+""".product_price (product_id, price, discount, currency_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET price = excluded.price, discount = excluded.discount, currency_id = excluded.currency_id
        RETURNING product_id, price
        """
        try:
            conn = get_connection(self.pool)
            cur = conn.cursor()
            currency_id = self.insert_currency(currency)
            cur.execute(sql, (product_id, price, discount, currency_id, ))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
        else:
            self.pool.putconn(conn)
            return product_id, price
        return None

    def insert_product_rating(self, product_id, rating_star, rating_count):
        sql = "INSERT INTO "+self.db_version+""".product_rating (product_id, rating_star, rating_count)
        VALUES (%s, %s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET rating_star = excluded.rating_star, rating_count = excluded.rating_count
        RETURNING product_id, rating_star, rating_count
        """
        try:
            conn = get_connection(self.pool)
            cur = conn.cursor()
            cur.execute(sql, (product_id, rating_star, rating_count, ))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
        else:
            self.pool.putconn(conn)
            return product_id, rating_star, rating_count
        return None

    def insert_product_feedback(self, product_id, feedback_count):
        sql = "INSERT INTO "+self.db_version+""".product_feedback (product_id, feedback_count)
        VALUES (%s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET feedback_count = excluded.feedback_count
        RETURNING product_id, feedback_count
        """
        try:
            conn = get_connection(self.pool)
            cur = conn.cursor()
            cur.execute(sql, (product_id, feedback_count, ))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
        else:
            self.pool.putconn(conn)
            return product_id, feedback_count
        return None

    def insert_product_quantity(self, product_id, sold, stock):
        sql = "INSERT INTO "+self.db_version+""".product_quantity (product_id, sold, stock)
        VALUES (%s, %s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET sold= excluded.sold, stock = excluded.stock
        RETURNING product_id, sold, stock
        """
        try:
            conn = get_connection(self.pool)
            cur = conn.cursor()
            cur.execute(sql, (product_id, sold, stock, ))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
        else:
            self.pool.putconn(conn)
            return product_id, sold, stock
        return None

    def insert_product_time(self, product_id, fetched_time):
        sql = "INSERT INTO "+self.db_version+""".product_time (product_id, fetched_time)
        VALUES (%s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET fetched_time = excluded.fetched_time
        RETURNING product_id, fetched_time
        """
        try:
            conn = get_connection(self.pool)
            cur = conn.cursor()
            cur.execute(sql, (product_id, fetched_time, ))
            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            
        else:
            self.pool.putconn(conn)
            return product_id, fetched_time
        return None
