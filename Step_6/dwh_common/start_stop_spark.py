import os
from pyspark.sql import SparkSession


# start Spark session
def spark_start():
    os.environ["JAVA_HOME"] = "/usr"
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
    os.environ["PYSPARK_SUBMIT_ARGS"] = """--driver-class-path /usr/share/java/mysql-connector-java.jar --jars /usr/share/java/mysql-connector-java.jar pyspark-shell"""
    
    spark = SparkSession.builder \
                        .master("yarn-client") \
                        .appName("spark_airflow") \
                        .config("hive.metastore.uris", "thrift://10.93.1.9:9083") \
                        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
                        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
                        .enableHiveSupport() \
                        .getOrCreate()

# stop Spark session
def spark_stop():
    spark.stop() 
