# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)

export AIRFLOW_HOME=/Users/lthoangg/Desktop/code/DataPipeline/airflow

# initialize the database
airflow db init

airflow users create \
    --username admin \
    --firstname Hoang \
    --lastname Le \
    --role Admin \
    --email letronghoang00@gmail.com
