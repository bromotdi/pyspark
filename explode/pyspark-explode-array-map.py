import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import explode_outer
from pyspark.sql.functions import posexplode
from pyspark.sql.functions import posexplode_outer

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})
        ]
df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show()

df2 = df.select(df.name,explode(df.knownLanguages))
df2.printSchema()
df2.show()

df3 = df.select(df.name,explode(df.properties))
df3.printSchema()
df3.show()

""" with array """
df.select(df.name,explode_outer(df.knownLanguages)).show()

""" with map """
df.select(df.name,explode_outer(df.properties)).show()

""" with array """
df.select(df.name,posexplode(df.knownLanguages)).show()

""" with map """
df.select(df.name,posexplode(df.properties)).show()

""" with array """
df.select(df.name,posexplode_outer(df.knownLanguages)).show()

""" with map """
df.select(df.name,posexplode_outer(df.properties)).show()
