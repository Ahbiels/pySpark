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
string_to_date.select(Func.year("Date")).show()
string_to_date.select("Name").orderBy(Func.year("Date")).show()
string_to_date.select("Date").groupBy(Func.year("Date")).count().show()

