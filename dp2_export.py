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

jdbc_aps = JdbcHook(jdbc_conn_id='jdbc_aps')
jdbc_243 = JdbcHook(jdbc_conn_id='jdbc_243')

limit = 10000000
offset = 500000
chunks = 500000

with DAG('dp2_export',default_args=args,schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')

    def export_db(**context):
        try:
            m = context['dag_run'].conf['m']
        except:
            m=1

        if m is None:
            m=1
        print('mode : '+ str(m))
        
        page = math.ceil(limit / offset)
        print('cara ' + str(m))
        if m == 1:
            for i in range(page):
                #mode = 'append'
                #if i == 0:
                #    mode = 'replace'
                sql = 'SELECT * FROM DEV2_DORA_ODS.dbo.ism_export2 WHERE RowID > {} AND RowID <= {}'.format(i*offset,(i+1)*offset)
                print('sql {} : {}'.format(i,sql))          
                rows = jdbc_aps.get_records(sql)
                
                sql = """INSERT INTO [dbo].[ism_account]([RowID],[ACCOUNT_ID],[GLOBAL_ACCOUNT_NUM],[CUSTOMER_ID],[ACCOUNT_STATE_ID],[ACCOUNT_TYPE_ID],[OFFICE_ID]"""
                sql += """,[PERSONNEL_ID],[CREATED_BY],[CREATED_DATE]"""
                sql +=""",[UPDATED_BY],[UPDATED_DATE],[CLOSED_DATE],[VERSION_NO],[OFFSETTING_ALLOWABLE],[EXTERNAL_ID],[SrcSystem],[DTPopulate],[SysUpdate])"""
                sql += """VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

                curs = jdbc_243.get_cursor()
                curs.executemany(sql,rows)
                curs.close()

                del curs

                
    def export_csv(**context):
        try:
            m = context['dag_run'].conf['m']
        except:
            m=1

        if m is None:
            m=1
        print('mode : '+ str(m))
        
        page = math.ceil(limit / offset)
        path = '/home/eeng/'

        print('cara ' + str(m))
        if m == 1:
            for i in range(page):
                sql = 'SELECT * FROM DEV2_DORA_ODS.dbo.ism_export2 WHERE RowID > {} AND RowID <= {}'.format(i*offset,(i+1)*offset)
                print('sql {} : {}'.format(i,sql))
                df = jdbc_aps.get_pandas_df(sql)
                df.to_csv(path + 'csv_cara_1_' + str(i+1),chunksize=chunks)
                del df

        if m == 2:
            for i in range(page):
                sql = 'SELECT * FROM DEV2_DORA_ODS.dbo.ism_export2 WHERE RowID > {} AND RowID <= {}'.format(i*offset,(i+1)*offset)
                curs = jdbc_aps.get_cursor()
                curs.execute(sql)
                row = curs.fetchone()
                f = open(path + 'csv_cara_2_' + str(i+1),'w+')
                while row:
                    f.writelines(str(row) + '\n')
                    row = curs.fetchone()

                f.close()
                curs.close()
                del f
                del curs

        if m == 3:
            sql = 'SELECT TOP {} * FROM DEV2_DORA_ODS.dbo.ism_export2'.format(limit)
            curs = jdbc_aps.get_cursor()
            curs.execute(sql)
            row = curs.fetchone()
            f = open(path + 'csv_cara_3', 'w+')
            while row:
                f.writelines(str(row) + '\n')
                row = curs.fetchone()

            f.close()
            curs.close()

            del f
            del curs

    def create_table():
        curs = jdbc_aps.get_cursor()
        sql = 'EXEC dbo.SPGenerateTable_ism'
        curs.execute(sql)

        curs.close()

    #create_table = PythonOperator(
    #    task_id='create_table',
    #    python_callable=create_table
    #) 

    export_csv = PythonOperator(
        task_id='export_csv',
        python_callable=export_csv
    )

    export_db = PythonOperator(
        task_id='export_db',
        python_callable=export_db
    )
start >> export_db >> export_csv
