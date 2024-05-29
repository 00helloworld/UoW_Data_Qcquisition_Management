# Import libararies
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Define Default arguments
default_args = {
    'owner': 'yanshen',
    'start_date': days_ago(0),
    'email': ['yanshen0406@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define DAG
dag = DAG(
    dag_id='demo-etl-dag',
    default_args=default_args,
    description='sss',
    schedule_interval=timedelta(days=1)
)

# Define Tasks
extract = BashOperator(task_id='extract', bash_command='echo "extract work"', dag=dag)
transform = BashOperator(task_id='transform', bash_command='echo "transform work"', dag=dag)
load = BashOperator(task_id='load', bash_command='echo "load work"', dag=dag)


# Define Pipeline
extract >> transform >> load