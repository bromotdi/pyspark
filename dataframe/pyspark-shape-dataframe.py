import pyspark
from pyspark.sql import SparkSession
import pandas as pd   

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()
data = [('Scott', 50), ('Jeff', 45), ('Thomas', 54),('Ann',34)] 
sparkDF=spark.createDataFrame(data,["name","age"]) 
sparkDF.printSchema()
sparkDF.show()

print((sparkDF.count(), len(sparkDF.columns)))

def sparkShape(dataFrame):
    return (dataFrame.count(), len(dataFrame.columns))

pyspark.sql.dataframe.DataFrame.shape = sparkShape
print(sparkDF.shape())

pandasDF=sparkDF.toPandas()
print(pandasDF.shape)
