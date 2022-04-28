from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('my_spark').getOrCreate()

limit=10000000

sql_aps = 'SELECT TOP {} * FROM DEV2_DORA_ODS.PROSPERA.ACCOUNT'.format(limit)
jdbc_url_aps = 'jdbc:sqlserver://10.8.10.11:17001;database=DEV2_DORA_ODS;username=12023227;password=P@ssw0rd'
df = spark.read.format("jdbc") \
    .option("url", jdbc_url_aps) \
    .option("query", sql_aps) \
    .load() \

df.write \
    .option('header',True) \
    .csv('/home/eeng/csv_spark')