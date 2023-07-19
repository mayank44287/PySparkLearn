from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local[3]")\
        .appName("SparkSQLTableDemo")\
        .enableHiveSupport()\
        .getOrCreate()

    flightTimeParquetDF = spark.read\
        .format("parquet")\
        .load("datasource/")

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")

    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTimeParquetDF\
        .write\
        .format("csv")\
        .bucketBy(5, "OP_CARRIER", "ORIGIN")\
        .mode("overwrite")\
        .saveAsTable("flight_data_tb1")

    spark.stop()

