import pyspark as pyspark
import os

os.environ['SPARK_HOME'] = '/home/ana/spark/spark-2.3.2-bin-hadoop2.7'
sc = pyspark.SparkContext.getOrCreate()
sc.setLogLevel("OFF")

sql_sc = pyspark.SQLContext(sc)


def get_data_from_file(file_path, sql_sc):
    data_df = (sql_sc
             .read.option('header', 'false')
             .option('sep', ';')
             .option('inferSchema', 'true')
             .csv(file_path))
    return data_df


prix2014_df = get_data_from_file("gas_data/Prix2014.csv", sql_sc)
prix2015_df = get_data_from_file("gas_data/Prix2015.csv", sql_sc)
prix2016_df = get_data_from_file("gas_data/Prix2016.csv", sql_sc)
prix2017_df = get_data_from_file("gas_data/Prix2017.csv", sql_sc)

services_df = get_data_from_file("gas_data/Services2017.csv", sql_sc)
stations_df = get_data_from_file("gas_data/Services2017.csv", sql_sc)