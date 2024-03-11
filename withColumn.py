from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as Func

spark = SparkSession.builder. \
    appName("WithColumn"). \
    getOrCreate()

schema_header = ["id","Name","City","Balance","Status"]
arqschema = "id INT, Name STRING, City STRING, Balance INT, Status STRING"
data = spark.read.csv("./data.csv", schema=arqschema, header="False")

data_new_colum = data.withColumn("Balancer_mult", Func.col("Balance") * 5).show()
cap_data = data.withColumn("Name", Func.initcap(Func.col("Name"))).show()