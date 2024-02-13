from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
          .appName('SparkByExamples.com') \
          .getOrCreate()
data = [("1","2023-07-01"),("2","2021-06-24"),("3","2022-08-24")]

df=spark.createDataFrame(data=data,schema=["id","date"])

df.select(
      col("date"),
      current_date().alias("current_date"),
      datediff(current_date(),col("date")).alias("datediff")
    ).show()

df.withColumn("datesDiff", datediff(current_date(),col("date"))) \
  .withColumn("montsDiff", months_between(current_date(),col("date"))) \
  .withColumn("montsDiff_round",round(months_between(current_date(),col("date")),2)) \
  .withColumn("yearsDiff",months_between(current_date(),col("date"))/lit(12)) \
  .withColumn("yearsDiff_round",round(months_between(current_date(),col("date"))/lit(12),2)) \
  .show()

data2 = [("1","07-01-2023"),("2","06-24-2023"),("3","08-24-2023")]  
df2=spark.createDataFrame(data=data2,schema=["id","date"])
df2.select(
    to_date(col("date"),"MM-dd-yyyy").alias("date"),
    current_date().alias("endDate")
    )

spark.sql("select round(months_between('2019-07-01',current_date())/12,2) as years_diff").show()
