from pyspark.sql import SparkSession
from benchmark_utils import Timer, BenchmarkLogger

# --- Inicializamos logger para guardar los tiempos ---
logger = BenchmarkLogger()

# --- Temporizador para creación de Spark Session ---
timer_cluster = Timer("Spark Cluster Startup")
timer_cluster.start()

spark = SparkSession.builder \
    .appName("Benchmark Test") \
    .master("spark://127.0.0.1:7077") \
    .getOrCreate()

timer_cluster_duration = timer_cluster.stop()
logger.log("Spark - Cluster startup", timer_cluster_duration)

# --- Datos de ejemplo ---
data = [(i, i ** 2) for i in range(10)]

# --- Temporizador para creación de DataFrame ---
timer_df = Timer("DataFrame Creation")
timer_df.start()

df = spark.createDataFrame(data, ["number", "square"])

timer_df_duration = timer_df.stop()
logger.log("Spark - Crear DataFrame", timer_df_duration)

# --- Temporizador para mostrar resultados ---
timer_show = Timer("Show DataFrame")
timer_show.start()

df.show()

timer_show_duration = timer_show.stop()
logger.log("Spark - Mostrar DataFrame", timer_show_duration)

# --- Apagar Spark ---
spark.stop()
