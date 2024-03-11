from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder. \
    appName("Read a file csv with schema"). \
    getOrCreate()

arqschema = "id INT, Name STRING, City STRING"
data = spark.read.csv("./data.csv", header=False, schema=arqschema)

data.show()
data.schema #terminal - visualiza detalhes sobre o schema