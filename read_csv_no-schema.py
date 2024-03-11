from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder. \
    appName("Read a file csv without schema"). \
    getOrCreate()

data = spark.read.load('./data.csv', 
                        format="csv", 
                        sep=",", 
                        header=False, 
                        inferschema=True
                    )
data.show()
data.schema #terminal - visualiza detalhes sobre o schema