from pyspark.sql import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local[3]")\
        .appName("ShuffleJoinDemo")\
        .getOrCreate()

    df1 = spark.read.json("data/d1/")
    df2 = spark.read.json("data/d2/")

    # comment this out once the tables is created
    # spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    # spark.sql("USE MY_DB")
    #
    # df1.coalesce(1).write \
    #     .bucketBy(3, "id") \
    #     .mode("overwrite") \
    #     .saveAsTable("MY_DB.flight_data1")
    #
    # df2.coalesce(1).write \
    #     .bucketBy(3, "id") \
    #     .mode("overwrite") \
    #     .saveAsTable("MY_DB.flight_data2")

    df3 = spark.read.table("my_db.flight_data1")
    df4 = spark.read.table("my_db.flight_data2")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    join_expr = df3.id == df4.id
    join_df = df3.join(df4, join_expr, "inner")

    join_df.collect()
    input("press a key to stop...")

