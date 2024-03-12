from pyspark.sql import SparkSession, functions as Func
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Exports datas").getOrCreate()

schema = "Name STRING, Balance STRING, Age INT, Date STRING, Job STRING"
data = spark.read.csv("./data.csv", header=False, schema=schema)

data = data.withColumn("Balance", Func.regexp_replace('Balance',"R\\$",""))
data = data.withColumn("Balance", Func.regexp_replace("Balance", "\\.","").cast(IntegerType()))
data = data.withColumn("Date", Func.to_date("Date", "dd/MM/yyyy"))
data = data.orderBy(Func.year("Date").desc())
data = data.withColumn("Maioridade", Func.when(data["Age"] > 18, "Maior de idade").otherwise("Menor de idade"))

try:
    group_work = data.groupBy("Job").agg(sum("Balance"))
    group_work.show()
except:
    print("Erro")

data.write.format("parquet").save("./data-parquet")
# data.write.format("csv").save("./datacsv")