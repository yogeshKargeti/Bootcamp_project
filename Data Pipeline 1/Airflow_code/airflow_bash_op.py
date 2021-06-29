from datetime import timedelta
import datetime

import airflow
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

default_args = {
    'owner':'Airflow',
    'start_date': datetime.datetime(2021,5,29),
}

dag = DAG(
    'spark_run',
    description='Spark run dag',
    default_args=default_args,
    schedule_interval='05 11 * * *'
)
t1= BashOperator(
    task_id='run_spark_submit',
    bash_command='spark-submit --class HelloWorld --master local[*] --conf spark.driver.extraJavaOptions="-Dcom.amazonaws.services.s3.enableV4"
    --conf spark.executor.extraJavaOptions="-Dcom.amazonaws.services.s3.enableV4" /home/ttn/IdeaProjects/EndProject3/target/scala-2.11/EndProject-assembly-0.1.jar',
    email= 'yogikargeti@gmail.com',
    email_on_failure= False,
    dag= dag
)

t2=BashOperator(
    task_id='run_sql_job',
    bash_command='mysql -u user ques2 < make_views.sql',
    email= 'yogikargeti@gmail.com',
    email_on_failure= False,
    dag= dag
)


t1 >> t2
