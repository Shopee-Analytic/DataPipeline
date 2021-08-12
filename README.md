# ShopeeCrawler
Crawl data from Shopee and visualize 

## Instructions
1. Create a *db-config.yml* in `dags/config`:
```yml
mongo:
  admin:
    url:
    usernamme:
    password:
  read_only:
    url:
    username:
    password:
  read_and_write:
    url:
    username:
    password:

postgre:
  admin:
    host:
    database: 
    user: 
    password:
    port:
    url:
```

2. Modify *config.yml* or *config-with-ariflow.yml* in `dags/config.yml` for crawler:
```yml
links: # link by each category to crawl
- https://shopee.vn/dien-thoai-phu-kien-cat.84
- https://shopee.vn/Th%E1%BB%9Di-Trang-Nam-cat.78
- https://shopee.vn/M%C3%A1y-t%C3%ADnh-Laptop-cat.13030
pages: 80 # Number of page of that category
```

3. Using one of two ways below: Docker with airflow / Locally with threadpool
### Using docker:
1. Create a _.env_ in base directory contains airflow's config:
```env
AIRFLOW_IMAGE_NAME=
AIRFLOW__CORE__EXECUTOR=CeleryExecutor

# Database
AIRFLOW__CORE__SQL_ALCHEMY_CONN=
AIRFLOW__CELERY__RESULT_BACKEND=
AIRFLOW__CELERY__BROKER_URL=

# Airflow ID
AIRFLOW_UID=
AIRFLOW_GID=

# Airflow WEBSERVER
_AIRFLOW_WWW_USER_USERNAME=
_AIRFLOW_WWW_USER_PASSWORD=

# Airflow UPDATE DATA
_AIRFLOW_DB_UPGRADE=
_AIRFLOW_WWW_USER_CREATE=

# Postgre's config
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=
```
2. Create database for scheduler
```cli
docker compose up (-d) airflow-init
```
3. Start all build-in services: posgre, redis, flower, webserver, scheduler, worker 
```cli
docker compose up (-d)
```
*Note:
- Add "-d" in command to run on background
- There're pretty much services so minimum required is 8gb of ram.


### Using local CLI:
1. Install all needed packages
```cli
pip/pip3 install -r requirements.txt
```
2. Start etl1
```cli
python/python3 local.py --etl 1
```
3. Start etl2
```cli
python/python3 local.py --etl 2
```
