from pyspark.sql import SparkSession

# Inicializar Spark
spark = SparkSession.builder \
    .appName("Test_Spark_Basic") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# Crear un DataFrame simple
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Mostrar contenido
df.show()

# Contar registros
print(f"🔢 Número de filas: {df.count()}")

# Detener Spark
spark.stop()
