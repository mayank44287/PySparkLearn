from pyspark.sql import *
from collections import namedtuple
import sys
from pyspark.sql import *
#from lib.logger import Log4j
from lib.utils import *

SurveyRecord = namedtuple("SurveyRecord",["Age", "Gender", "country", "State"])

if __name__ == "__main__":
    conf = SparkConf().setMaster("local[3]").setAppName("HelloSparkSQL")
    spark = SparkSession \
        .builder \
        .config(conf= conf) \
        .getOrCreate()

    sc = spark.sparkContext

    file = "data/sample.csv"

    linesRDD = sc.textFile(file)

    partitionedRDD = linesRDD.repartition(2)

    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = selectRDD.filter(lambda r: r.age < 40)
    kvRDD = selectRDD.map(lambda r: (r.country, 1))
    countRDD = kvRDD.reduceByKey(lambda x, y: x + y)

    colsList = countRDD.collect()
    for x in colsList:
        print(x)

    spark.stop()
