from pyspark.sql import SparkSession
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName("Soma de valores").getOrCreate()

schema = "Products STRING, Price INT"
data = [["Celular",540],['NoteBook',2340],["Celular",400]]

df = spark.createDataFrame(data, schema)

gather = df.groupBy("Products").agg(sum("Price"))

# groupBy("Produtos"): Esta função é usada para agrupar as linhas que têm 
# o mesmo valor na coluna especificada, neste caso, "Produtos". Isso 
# significa que todas as linhas com o mesmo nome de produto serão 
# agrupadas juntas para a próxima etapa de agregação.

gather.show()
df.show()
