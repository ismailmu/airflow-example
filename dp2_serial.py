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

with DAG(dag_id='dp2_serial',default_args=args) as dag:
    start = DummyOperator(
        task_id = 'start'
    )
    end_section_1 = DummyOperator(
        task_id = 'end_section_1'
    )
    end = DummyOperator(
        task_id = 'end'
    )
    task_groups = []
    for group in flows:
        task_group = DummyOperator(task_id = 'group_' + group)
        end_group = DummyOperator(task_id = 'end_group_' + group)

        task_groups.append(task_group)
        tables = flows[group]
        table_groups = []
        sp_groups = {}
        for table in tables:
            table_group = DummyOperator(task_id = table)
            table_groups.append(table_group)
            
            sps = tables[table]
            sp_groups_temp = []
            for sp in sps:
                sp_group = DummyOperator(task_id = sp)
                sp_groups_temp.append(sp_group)
            
            sp_groups[table] = sp_groups_temp

        len_tables = len(table_groups)
        for i in range(len_tables):
            table = table_groups[i]
            sps = sp_groups[table.task_id]
            end_table = DummyOperator(task_id = 'end_' + table.task_id)


            if len(sps) > 0:
                table >> sps >> end_table
            else:
                table >> end_table

            if i==0:
                task_group >> table

                if (i+1) < len_tables:
                    end_table >> table_groups[i+1]
                else:
                    end_table >> end_group >> end_section_1 >> end
            elif i < len_tables - 1:
                end_table >> table_groups[i+1]
            else:
                end_table >> end_group >> end_section_1 >> end
            
start >> task_groups