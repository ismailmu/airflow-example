from airflow.models.dag import DAG
from airflow.utils.dates import days_ago

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

args = {
    'owner': 'eeng',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG('test_xcom',default_args=args,schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')

    def send_data(**kwargs):
        return "from t1"

    def get_data(**kwargs):
        for key in kwargs:
            print(key)
        ti = kwargs['ti']
        value = ti.xcom_pull(task_ids='t1')
        print(value)
    
    t1 = PythonOperator(
        task_id='t1',
        python_callable=send_data
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=get_data
    )

start >> t1 >> t2