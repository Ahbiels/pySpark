from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.appName("Select and expr").getOrCreate()
schema = "Name STRING, Age INT, Job STRING, Saldo INT" #Funciona
data = [("Angelo",19,"Data Enginner",230),("Joao",20,"Programador",298)]

df = spark.createDataFrame(data,schema)
df.select("Job","Name","Age",expr("Saldo * 2")).show()

spark.stop()