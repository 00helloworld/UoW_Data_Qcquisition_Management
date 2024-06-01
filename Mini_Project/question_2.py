'''
1. cp to DAGs path
2. restart airflow: airflow standalone
3. excute
'''

# Import libararies
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

# Config snowflake
DATA_URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv"
DATA_PATH = "/Users/dongyanshen/Desktop/DYS/UoW_AI_Course/Data_Management/Mini_Project"
CSV = "pima-indians-diabetes.csv"
TRANSFORMED_CSV = "transformed_data.csv"
SNOWFLAKE_DB = "YANSHEN_DIT"
SNOWFLAKE_SCHEMA = "MINI_PROJECT"
SNOWFLAKE_TABLE = "DIABETES"
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
    dag_id='Mini_Project_Q2',
    default_args=default_args,
    description='Mini_Project_Q2',
    schedule_interval=timedelta(hours=1),
    catchup=False
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
        Pregnancies NUMBER,
        Glucose NUMBER,
        BloodPressure NUMBER,
        SkinThickness NUMBER,
        Insulin NUMBER,
        BMI FLOAT,
        DiabetesPedigreeFunction FLOAT,
        Age NUMBER,
        Outcome NUMBER
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
        curl --output {DATA_PATH}/{CSV} {DATA_URL};
        
    '''
)

# Transform
def transform_data():
    header_list = ['Pregnancies', 'Glucose', 'BloodPressure', 'SkinThickness', 
                   'Insulin', 'BMI', 'DiabetesPedigreeFunction', 'Age', 'Outcome']
    data = pd.read_csv(f'{DATA_PATH}/{CSV}', header=None, names=header_list)
    transformed_data = data[data.Glucose > 120]
    transformed_data.to_csv(f'{DATA_PATH}/{TRANSFORMED_CSV}', index=False)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag
)

# Load
load = BashOperator(
    task_id = 'load',
    dag = dag,
    bash_command = f'''
        echo "Loading to Snowflake...";
        {SNOWSQL_CMD} -q "PUT file://{DATA_PATH}/{TRANSFORMED_CSV} @%{SNOWFLAKE_TABLE}";
        {SNOWSQL_CMD} -q "COPY INTO {SNOWFLAKE_TABLE}
                        FROM @%{SNOWFLAKE_TABLE}/{TRANSFORMED_CSV}
                        FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);"
    '''
)

# Clear
clear = BashOperator(
    task_id = 'clear',
    dag = dag,
    bash_command = f'''
        echo "Clearing...";
        rm -f {DATA_PATH}/{CSV} {DATA_PATH}/{TRANSFORMED_CSV};
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