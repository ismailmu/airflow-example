import math
import json
from datetime import datetime
import pandas as pd

import jaydebeapi

config=[]
with open('test_jdbc.json',mode='r') as f:
    config = json.load(f)

driver_aps = config['driver_aps']
url_aps = config['url_aps']
cre_aps = config['cre_aps']
driver_path_aps = config['driver_path_aps']

driver_sql = config['driver_sql']
url_sql = config['url_sql']
cre_sql = config['cre_sql']
driver_path_sql = config['driver_path_sql']

path = config['path']
offset = 100000
limit = 10000000
page = math.ceil(limit / offset)

m = config['m']
print('mode : ' + str(m))

if m==1:
    for i in range(page):
        dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = 'SELECT * FROM DEV2_DORA_ODS.dbo.ism_export2 WHERE RowID > {} AND RowID <= {}'.format(i*offset,(i+1)*offset)
        print('sql : ' + sql)
        print('start page {} : {}'.format(i+1,dt))
        try:
            conn = jaydebeapi.connect(driver_aps,url_aps,cre_aps,driver_path_aps)
            curs = conn.cursor()
            curs.execute(sql)
            row = curs.fetchone()
            f = open(path + 'csv__' + str(i+1),'w+')
            while row:
                f.write(str(row) + '\n')
                row = curs.fetchone()
            
            dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('end page {} : {}'.format(i+1,dt))

            f.close()
            curs.close()
            conn.close()
        except BaseException as e:
            print(e)

if m==2:
    for i in range(page):
        sql = 'SELECT * FROM DEV2_DORA_ODS.dbo.ism_export2 WHERE RowID > {} AND RowID <= {}'.format(i*offset,(i+1)*offset)
        conn = jaydebeapi.connect(driver_aps,url_aps,cre_aps,driver_path_aps)
        #curs = conn.cursor()
        #curs.execute(sql)
        df = pd.read_sql(sql,conn)
        df.to_csv(path + 'csv__' + str(i+1))
       
        df = None
        
        #curs.close()
        conn.close()

if m==3:
    for i in range(page):
        try:
            conn2 = jaydebeapi.connect(driver_sql,url_sql,cre_sql,driver_path_sql)

            dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql = 'SELECT * FROM DEV2_DORA_ODS.dbo.ism_export2 WHERE RowID > {} AND RowID <= {}'.format(i*offset,(i+1)*offset)
            print('sql : ' + sql)
            print('start page {} : {}'.format(i+1,dt))
            conn = jaydebeapi.connect(driver_aps,url_aps,cre_aps,driver_path_aps)
            curs = conn.cursor()
            curs.execute(sql)
            rows = curs.fetchall()
            #print(rows)
            curs.close()
            conn.close()

            sql = """INSERT INTO [dbo].[ism_account]([RowID],[ACCOUNT_ID],[GLOBAL_ACCOUNT_NUM],[CUSTOMER_ID],[ACCOUNT_STATE_ID],[ACCOUNT_TYPE_ID],[OFFICE_ID]"""
            sql += """,[PERSONNEL_ID],[CREATED_BY],[CREATED_DATE]"""
            sql +=""",[UPDATED_BY],[UPDATED_DATE],[CLOSED_DATE],[VERSION_NO],[OFFSETTING_ALLOWABLE],[EXTERNAL_ID],[SrcSystem],[DTPopulate],[SysUpdate])"""
            sql += """VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            curs2 = conn2.cursor()
            curs2.executemany(sql,rows)
            
            curs2.close()
            conn2.close()
            dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('end page {} : {}'.format(i+1,dt))
        except Exception as e:
            print(e)

if m==4:
    j=1
    for i in range(page):
        try:
            conn2 = jaydebeapi.connect(driver_sql,url_sql,cre_sql,driver_path_sql)
            dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql = 'SELECT * FROM DEV2_DORA_ODS.dbo.ism_export2 WHERE RowID > {} AND RowID <= {}'.format(i*offset,(i+1)*offset)
            print('sql : ' + sql)
            print('start page {} : {}'.format(i+1,dt))
            conn = jaydebeapi.connect(driver_aps,url_aps,cre_aps,driver_path_aps)
            curs = conn.cursor()
            curs.execute(sql)
            row = curs.fetchone()
            sql = """INSERT INTO [dbo].[ism_account]([RowID],[ACCOUNT_ID],[GLOBAL_ACCOUNT_NUM],[CUSTOMER_ID],[ACCOUNT_STATE_ID],[ACCOUNT_TYPE_ID],[OFFICE_ID]"""
            sql += """,[PERSONNEL_ID],[CREATED_BY],[CREATED_DATE]"""
            sql +=""",[UPDATED_BY],[UPDATED_DATE],[CLOSED_DATE],[VERSION_NO],[OFFSETTING_ALLOWABLE],[EXTERNAL_ID],[SrcSystem],[DTPopulate],[SysUpdate])"""
            sql += """VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
            
            curs2 = conn2.cursor()
            while row:
                curs2.executemany(sql,row)
                j+=1
                if j >= offset:
                    conn2.commit()
                    j=1
                row = curs.fetchone()
            
            curs2.close()

            curs.close()
            conn.close()

            conn2.close()
            dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print('end page {} : {}'.format(i+1,dt))
        except Exception as e:
            print(e)
