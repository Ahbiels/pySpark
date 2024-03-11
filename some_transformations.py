from pyspark.sql import SparkSession, functions as Func
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder. \
    appName("Dates"). \
    getOrCreate()

schema_header = ["id","Name","City","Balance","Status","Date"]
arqschema = "id INT, Name STRING, City STRING, Balance INT, Status STRING, Date STRING"
data = spark.read.csv("./data_with_date.csv", schema=arqschema, header="False")

string_to_date = data.withColumn("Date", Func.to_date("Date", "MM-dd-yyyy"))

string_to_date.show()
string_to_date.take(1)
string_to_date.count()
string_to_date.orderBy("Balance").show() #crescente
string_to_date.orderBy(Func.col("Balance").desc()).show() #decrescente
string_to_date.orderBy(Func.year("Date"),Func.col("Balance")).show()
string_to_date.filter(Func.year("Date") == '2014').show()