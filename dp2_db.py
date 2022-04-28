import math

import pandas as pd

from airflow.utils.dates import days_ago

from airflow.models.dag import DAG

from airflow.providers.jdbc.hooks.jdbc import JdbcHook

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

aps = JdbcHook(jdbc_conn_id='aps_jdbc')
local_mssql = JdbcHook(jdbc_conn_id='local_mssql')

limit = 10000000
offset = 1000000
chunks = 500000
table_name = 'dbo.ism_account'

with DAG('dp2_db',default_args=args,schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')

    def get_data(**context):
        try:
            m = context['dag_run'].conf['m']
        except:
            m=1

        if m is None:
            m=1
        print('mode : '+ str(m))
        
        
        page = math.ceil(limit / offset)
        print('cara ' + str(m))
        if m==1:
            for i in range(page):
                sql = 'SELECT * FROM DEV2_DORA_ODS.dbo.ism_export WHERE RowID > {} AND RowID <= {}'.format(i*offset,(i+1)*offset)
                print('sql {} : {}'.format(m,sql))
                df = aps.get_pandas_df(sql)
                truncate='append'
                if (i==0):
                    truncate='replace'

                df.to_sql(table_name,if_exists=truncate,con=local_mssql)

        if m == 2:
            for i in range(page):
                sql = 'SELECT top 1 * FROM DEV2_DORA_ODS.dbo.ism_export WHERE RowID > {} AND RowID <= {}'.format(i*offset,(i+1)*offset)
                curs = aps.get_cursor()
                curs.execute(sql)
                row = curs.fetchone()
                while row:
                    print(str(row))
                    #local_mssql.execute(sql)
                    row = curs.fetchone()

                curs.close()

    export_sqlserver = PythonOperator(
        task_id='db_sql_server',
        python_callable=get_data
    )

start >> export_sqlserver
