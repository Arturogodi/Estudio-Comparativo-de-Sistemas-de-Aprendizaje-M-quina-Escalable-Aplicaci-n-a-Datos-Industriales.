from pyspark.sql import SparkSession

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("TestSpark") \
    .master("spark://localhost:7077") \
    .getOrCreate()

# Crear datos de prueba
data = [(x, x * x) for x in range(10)]

# Crear DataFrame
df = spark.createDataFrame(data, ["number", "square"])

# Mostrar resultados
df.show()

# Parar Spark
spark.stop()
