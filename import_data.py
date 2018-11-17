import pyspark as pyspark
import os

os.environ['SPARK_HOME'] = '/home/ana/spark/spark-2.3.2-bin-hadoop2.7'
sc = pyspark.SparkContext.getOrCreate()
sc.setLogLevel("OFF")

sql_sc = pyspark.SQLContext(sc)

prices2014_df = (sql_sc
             .read.option('header', 'false')
             .option('sep', ';')
             .option('inferSchema', 'true')
             .csv('gas_data/Prix2014.csv'))
