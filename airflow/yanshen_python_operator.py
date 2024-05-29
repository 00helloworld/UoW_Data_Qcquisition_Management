# Import libararies
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Define Default arguments
default_args = {
    'owner': 'yanshen',
    'start_date': datetime(2024, 5, 27),
    'schedule_interval': '@daily',
    'email': ['yanshen0406@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def extract_data():
    print('Extracting...')

def transform_data():
    print('Transforming...')

def load_data():
    print('Loading...')

# Define DAG
dag = DAG(
    dag_id='demo-etl-dag-python',
    default_args=default_args,
    description='sss'
)


# Tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data,
    dag=dag
)

extract_task >> transform_task >> load_task