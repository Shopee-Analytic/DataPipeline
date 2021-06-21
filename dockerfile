FROM apache/airflow:latest-python3.8

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt --no-cache-dir