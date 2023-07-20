from pyspark.sql import *
from collections import namedtuple
import sys
from pyspark.sql import *
#from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("HelloSparkSQL")
    spark = SparkSession \
        .builder \
        .config(conf=conf) \
        .getOrCreate()

    file = "data/sample.csv"

    surveyDf = spark.read.option("header", "true").option("inferSchema", "true").csv(file)

    # we first need tp create a view for sql command to work on our data
    surveyDf.createOrReplaceTempView("surveyTable")

    countDf = spark.sql("select Country, count(1) as count from surveyTable where Age<40 group by Country")
    countDf.show()

    spark.stop()