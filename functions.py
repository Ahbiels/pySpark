from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as Func

spark = SparkSession.builder. \
    appName("Creates conditions"). \
    getOrCreate()

schema_header = ["id","Name","City","Balance","Status"]
arqschema = "id INT, Name STRING, City STRING, Balance INT, Status STRING"
data = spark.read.csv("./data.csv", schema=arqschema, header="False")

data.select(Func.avg("Balance")).show()#Media do saldo
data.select(Func.count("Balance")).show()#Conta as ocorrencias
data.select(Func.sum("Balance")).show()#Soma os valors
data.select(Func.max("Balance")).show()#Mostra o valor maais alto
data.select(Func.min("Balance")).show()#Mostra o valor mais baixo

