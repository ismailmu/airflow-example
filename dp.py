from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    'dp',
    default_args=default_args,
    description='Get Data prospera from ODS',
    schedule_interval=timedelta(days=1),
)

#Creating JDBC connection using Conn ID
local_mssql = JdbcHook(jdbc_conn_id='local_mssql')

t_start = DummyOperator(
    task_id = 'start',
    dag = dag
)

t_transfer = DummyOperator(
    task_id = 'transfer',
    dag = dag
)

def call_sp(*args):
    group = args[0]
    table = args[1]
    print('call sp table : ' + table + ' in group : ' + group)

groups = local_mssql.get_records("SELECT DISTINCT group_name FROM TABLE_SETTING")

t_groups = []

for i in range(len(groups)):
    g = str(groups[i][0])
    t_groups.append(DummyOperator(
        task_id = 'run_group_' + g,
        dag = dag
    ))
    t_tables = []
    tables = local_mssql.get_records("SELECT TABLE_NAME FROM TABLE_SETTING WHERE IsActive = 1 AND GROUP_NAME = '" + g + "'")
    for j in range(len(tables)):
        h = str(tables[j][0])
        t_tables.append(PythonOperator(
            task_id = 'table_' + h,
            python_callable=call_sp,
            op_args=[g,h],
            dag = dag
        ))

        if j == 0:
            t_groups[i] >> t_tables[j]
        else: #j <= len(tables):
            t_tables[j-1] >> t_tables[j]
        
    t2 = DummyOperator(
        task_id = 'end_section',
        dag = dag
    )
    t_tables[len(tables)-1] >> t2 >> t_transfer

t_start >> t_groups