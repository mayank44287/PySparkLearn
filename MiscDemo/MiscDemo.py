from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Misc Demo") \
        .master("local[2]") \
        .getOrCreate()

    dataList = [("Ravi", "28", "1", "2002"),
                 ("Abdul", "23", "5", "81"),  # 1981
                 ("John", "12", "12", "6"),  # 2006
                 ("Rosy", "7", "8", "63"),  # 1963
                 ("Abdul", "23", "5", "81")]  # 1981

    # we can directly create df using this, and also assign col names using toDF function
    rawDf = spark.createDataFrame(dataList).toDF("name", "day", "month", "year").repartition(3)
    rawDf.printSchema()

    # adding a column to the DF (col name "id" which takes unique id through the in built monotoni.. function)
    df1 = rawDf.withColumn("id", monotonically_increasing_id())
    df1.show()

    # modifying year column to display proper year
    df2 = df1.withColumn("year", expr("""
    case
        when year < 21 then year + 2000
        when year < 100 then year + 1900
        else year
    end"""))
    df2.show()

    # the above transformation will lead to year field being promoted to float and then back to string
    # we explicitly cast the year as int
    df3 = df1.withColumn("year", expr("""
             case when year < 21 then cast(year as int) + 2000
             when year < 100 then cast(year as int) + 1900
             else year
             end"""))
    df3.show()

    # or we can cast explicitly using the following as well
    df4 = df1.withColumn("year", expr("""
             case when year < 21 then year + 2000
             when year < 100 then year + 1900
             else year
             end""").cast(IntegerType()))
    df4.show()
    df4.printSchema()

    # Or we can cast before the expression, as to improve the schema inference
    # best way is to create a custom schema in the beginning based on the data,
    # which makes tranformations easier later on
    df5 = df1.withColumn("day", col("day").cast(IntegerType())) \
        .withColumn("month", col("month").cast(IntegerType())) \
        .withColumn("year", col("year").cast(IntegerType()))

    df6 = df5.withColumn("year", expr("""
              case when year < 21 then year + 2000
              when year < 100 then year + 1900
              else year
              end"""))
    df6.show()

    # column object based transformation
    df7 = df5.withColumn("year", \
                         when(col("year") < 21, col("year") + 2000) \
                         .when(col("year") < 100, col("year") + 1900) \
                         .otherwise(col("year")))
    df7.show()

    # adding a column to the dataframe
    df8 = df7.withColumn("dob", expr("to_date(concat(day,'/',month,'/',year), 'd/M/y')"))
    df8.show()


    # adding column, dropping redundant day, month, year columns, dropduplicates, sort etc
    df9 = df7.withColumn("dob", to_date(expr("concat(day,'/',month,'/',year)"), 'd/M/y')) \
        .drop("day", "month", "year") \
        .dropDuplicates(["name", "dob"]) \
        .sort(expr("dob desc"))
    df9.show()
    spark.stop()