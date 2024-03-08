from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark Test").getOrCreate()

data = [("Angelo",19),("Thais",20),("Pedro",19),("Maria",18)]

df = spark.createDataFrame(data)

df.show()
spark.stop()