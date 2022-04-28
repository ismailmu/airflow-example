from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.models.dag import DAG
from airflow.models import Variable

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

args = {
    'owner': 'eeng',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(dag_id='test_trigger_param',default_args=args) as dag:
    start = DummyOperator(
        task_id = 'start'
    )

    def get_param(**context):
        print(context['dag_run'].conf['m'])


    test = PythonOperator(
        task_id='test',
        python_callable=get_param
    )
