import os
import shutil
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('my_spark_csv').getOrCreate()

path='/home/eeng/csv_spark'

limit=10000000


if os.path.exists(path):
    shutil.rmtree(path)
    #os.rmdir(path)

sql_aps = 'SELECT TOP {} * FROM DEV2_DORA_ODS.dbo.ism_export2'.format(limit)
jdbc_url_aps = 'jdbc:sqlserver://10.8.10.11:17001;database=DEV2_DORA_ODS;username=12023227;password=P@ssw0rd'
df = spark.read.format("jdbc") \
    .option("url", jdbc_url_aps) \
    .option("query", sql_aps) \
    .load() \

df.write \
    .option('header',True) \
    .csv(path)