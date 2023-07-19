from pyspark.sql import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local[3]")\
        .appName("ShuffleJoinDemo")\
        .getOrCreate()

    flightTimeDf1 = spark.read.json("data/d1/")
    flightTimeDf2 = spark.read.json("data/d2/")

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    joinExpr = flightTimeDf1.id == flightTimeDf2.id
    joinDf = flightTimeDf1.join(flightTimeDf2, joinExpr, "inner")

    joinDf.collect()
    input("press a key to stop")

    spark.stop()