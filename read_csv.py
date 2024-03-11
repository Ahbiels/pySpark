from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder. \
    appName("Read a file csv with schema"). \
    getOrCreate()

schema_header = ["id","Name","City","Balance","Status"]
arqschema = "id INT, Name STRING, City STRING, Balance STRING, Status STRING"
data = spark.read.csv("./data.csv", schema=arqschema, header="False")

data.show()
data.schema #terminal - visualiza detalhes sobre o schema