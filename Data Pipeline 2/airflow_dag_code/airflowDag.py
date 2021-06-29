from datetime import timedelta
import datetime

import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner':'Airflow',
    'start_date': datetime.datetime(2021,5,26),
}

dag = DAG(
    'hive_import',
    description='Running sql file into hive database',
    default_args=default_args,
    schedule_interval='30 18 6 * *'
)

hive_ex= BashOperator(
    task_id='execute sql file',
    bash_command='hive -f hdfs://usr/project/exec_hive.sql',
    email= 'yogikargeti@gmail.com',
    email_on_failure= False,
    dag= dag
)

hive_ex


