import math

import pandas as pd

from airflow.utils.dates import days_ago

from airflow.models.dag import DAG

from airflow.providers.jdbc.hooks.jdbc import JdbcHook

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

args = {
    'owner': 'eeng',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG('dp2_spark_export',default_args=args,schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')
 
    export_csv = BashOperator(
        task_id='export_csv',
        bash_command='/home/eeng/python/using_spark_local.sh /home/eeng/python/my_spark_csv.py'
    )

    export_db = BashOperator(
        task_id='export_db',
        bash_command='/home/eeng/python/using_spark_local.sh /home/eeng/python/my_spark_db.py'
    )
start >> export_csv >> export_db