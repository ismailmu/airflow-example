from airflow.utils.dates import days_ago
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
args = {
    'owner': 'eeng',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG('dp2_run',default_args=args,schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')
    t1 = BashOperator(
        task_id = 'run_dp2_serial',
        bash_command="airflow dags trigger dp2_serial"
    )

    start >> t1
