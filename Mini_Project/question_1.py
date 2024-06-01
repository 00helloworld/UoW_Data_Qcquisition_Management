# Import libararies
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Config snowflake
DATA_URL = "https://elasticbeanstalk-us-east-2-340729127361.s3.us-east-2.amazonaws.com/web-server-access-log.txt.gz"
DATA_PATH = "/Users/dongyanshen/Desktop/DYS/UoW_AI_Course/Data_Management/Mini_Project"
TEMP_GZ = "web-server-access-log.txt.gz"
TXT = "web-server-access-log.txt"
TRANSFORMED_CSV = "transformed_data.csv"
SNOWFLAKE_DB = "YANSHEN_DIT"
SNOWFLAKE_SCHEMA = "MINI_PROJECT"
SNOWFLAKE_TABLE = "SERVER_LOG"
SNOWSQL_CMD = f"snowsql -c my_connection -d {SNOWFLAKE_DB} -s {SNOWFLAKE_SCHEMA}"

# Define Default arguments
default_args = {
    'owner': 'yanshen',
    'start_date': days_ago(0),
    'email': ['yanshen0406@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Define DAG
dag = DAG(
    dag_id='Mini_Project_Q1',
    default_args=default_args,
    description='Mini_Project_Q1',
    schedule_interval=timedelta(days=1)
)

# Define Tasks

# Prepare
prepare = BashOperator(
    task_id = 'prepare',
    dag = dag,
    bash_command = f'''
        echo "Preparing...";
        echo "Creating Table on Snowflake...";
        {SNOWSQL_CMD} -q "CREATE OR REPLACE TABLE {SNOWFLAKE_TABLE} (
        timestamp TIMESTAMP,
        latitude FLOAT,
        longitude FLOAT,
        visitorid CHAR(37)
    );"
    '''
)
# Extract
extract = BashOperator(
    task_id = 'extract',
    dag = dag,
    bash_command = f'''
        echo "Extracting...";
        echo "Downloading File...";
        curl --output {DATA_PATH}/{TEMP_GZ} {DATA_URL};

        echo "Decompressing...";
        gunzip -f {DATA_PATH}/{TEMP_GZ} > {DATA_PATH}/{TXT};
        
    '''
)
# Transform
transform = BashOperator(
    task_id = 'transform',
    dag = dag,
    bash_command = f'''
        echo "Transforming...";
        cut -d '#' -f 1-4 {DATA_PATH}/{TXT} | tr '#' ',' > {DATA_PATH}/{TRANSFORMED_CSV};
        echo "Transform Done";
    '''
)
# Load
load = BashOperator(
    task_id = 'load',
    dag = dag,
    bash_command = f'''
        echo "Loading to Snowflake...";

    '''
)
# Clear
clear = BashOperator(
    task_id = 'clear',
    dag = dag,
    bash_command = f'''
        echo "Clearing...";

    '''
)


# Define Pipeline
prepare >> extract >> transform >> load >> clear


# bash_command = f'''
#         echo "Extracting...";
#         echo "Downloading File...";
#         curl --output {TEMP_GZ} {DATA_URL};

#         echo "Decompressing...";
#         gunzip {TEMP_GZ}
#     '''
# print(bash_command)