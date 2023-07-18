from pyspark.sql import *

import sys
from pyspark.sql import *
#from lib.logger import Log4j
from lib.utils import *

if __name__ == "__main__":

    conf = get_spark_app_config()
    spark = SparkSession \
        .builder \
        .config(conf= conf) \
        .getOrCreate()


    file = "data/sample.csv"
    survey_df = load_survey_df(spark, file)
    partitionedSurveyDf = survey_df.repartition(2)
    countDf = count_by_country(partitionedSurveyDf)

    out = countDf.collect()
    print(out)
    input("Press Enter")
    # logger.info("Finished HelloSpark")
    spark.stop()
