from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def toDateDf(df, fmt, fld):
    return df.withColumn(fld, to_date(fld, fmt))

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .master("local[3]")\
        .appName("RowDemo")\
        .getOrCreate()

    mySchema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())])

    # creating custom df for testing purpose
    myRows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"),
              Row("125", "04/5/2020"), Row("126", "4/05/2020")]

    myRDD = spark.sparkContext.parallelize(myRows, 2)
    myDF = spark.createDataFrame(myRDD, mySchema)

    myDF.printSchema()
    myDF.show()
    newDF = toDateDf(myDF, "M/d/y", "EventDate")
    newDF.printSchema()
    newDF.show()

    spark.stop()


