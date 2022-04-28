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

flows = Variable.get("flow",deserialize_json=True)

with DAG(dag_id='dp2',default_args=args) as dag:
    start = DummyOperator(
        task_id = 'start'
    )
    end = DummyOperator(
        task_id = 'end'
    )
    end_section_1 = DummyOperator(task_id = 'end_section_1')

    task_groups = []
    for group in flows:
        task_group = DummyOperator(task_id = 'group_' + group)
        tables = flows[group]
        table_groups = []
        for table in tables:
            table_group = DummyOperator(task_id = table)
            sps = flows[group][table]
            for sp in sps:
                sp_group = DummyOperator(task_id = sp)
                sp_group >> end_section_1 >> end
                table_group >> sp_group
            
            table_groups.append(table_group)
        
        task_group >> table_groups
        task_groups.append(task_group)

start >> task_groups
