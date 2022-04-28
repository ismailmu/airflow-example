import os
import shutil
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('my_spark_db').getOrCreate()

limit=10000000
fetch=500000
batch=500000

sql_aps = 'SELECT TOP {} * FROM DEV2_DORA_ODS.dbo.ism_export2'.format(limit)

jdbc_url_aps = 'jdbc:sqlserver://10.8.10.11:17001;database=DEV2_DORA_ODS;username=12023227;password=P@ssw0rd'
jdbc_url_243 = 'jdbc:sqlserver://10.8.10.243\\SQL17:1434;database=DPSource;username=sa;password=P@ssw0rd.'

df = spark.read.format("jdbc") \
    .option("url", jdbc_url_aps) \
    .option("query", sql_aps) \
    .option("fetchsize", fetch) \
    .load()

df.write.format('jdbc') \
    .option('url', jdbc_url_243) \
    .option('truncate', True) \
    .mode('Overwrite') \
    .option("dbtable", 'dbo.ism_account') \
    .option("batchsize", batch) \
    .save()