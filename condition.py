from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as Func

spark = SparkSession.builder. \
    appName("Creates conditions"). \
    getOrCreate()

schema_header = ["id","Name","City","Balance","Status"]
arqschema = "id INT, Name STRING, City STRING, Balance INT, Status STRING"
data = spark.read.csv("./data.csv", schema=arqschema, header="False")

#Conditions
data.select(*schema_header).where((Func.col("Balance") > 250) & (Func.col("Balance") < 1000)).show()
data.select(*schema_header).where(Func.col("Status") == "ATIVO").show()
