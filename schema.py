from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Schema").getOrCreate()

# schema = ["Name STRING","Age INT","Job STRING"] #Nao funciona
schema = "Name STRING, Age INT, Job STRING" #Funciona
data = [("Angelo",19,"Data Enginner"),("Joao",20,"Programador")]

df = spark.createDataFrame(data,"Name STRING, Age INT, Job STRING")

df.show()
spark.stop()