import re
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def parseGender(gender):
    femalePattern = r"^f$|f.m|w.m"
    malePattern = r"^m$|ma|m.l"
    if re.search(femalePattern, gender.lower()):
        return "Female"
    elif re.search(malePattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"

if __name__ =="__main__":
    spark = SparkSession.builder\
        .master("local[3]")\
        .appName("UDFDemo")\
        .getOrCreate()

    surveyDf = spark.read.option("header","true").option("inferSchema", "true").csv("data/survey.csv")
    # surveyDf.show(10)

    parseGenderUdf = udf(parseGender, StringType())
    surveyDf2 = surveyDf.withColumn("Gender", parseGenderUdf("Gender"))
    #surveyDf2 = surveyDf.withColumn("Gender", parseGenderUdf(surveyDf.Gender))
    surveyDf2.show()

    # To use UDF with SQL like expressions, the udf needs to be
    # registered as SQL function and it should go to the catalog

    spark.udf.register("parseGenderUDF", parseGender, StringType())

    surveyDf3 = surveyDf.withColumn("Gender", expr( "parseGenderUdf(Gender)"))
    surveyDf3.show(10)

    spark.stop()