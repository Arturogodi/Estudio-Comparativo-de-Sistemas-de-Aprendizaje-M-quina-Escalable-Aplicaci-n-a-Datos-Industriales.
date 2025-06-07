from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from glob import glob
import os

# ⚙️ Configura Spark con menor uso de recursos para evitar OOM
spark = SparkSession.builder \
    .appName("ETL_Clean_ByBatch") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.executor.memory", "512m") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# 📁 Paths
input_path = "/data/silver/consolidado/"
output_path = "/data/silver/Taxi_NYC/yellow_tripdata_cleaned/"

# 🔍 Encuentra todos los archivos parquet en el directorio
all_files = glob(f"{input_path}/*.parquet")

# 🔁 Procesamiento por lotes (archivo por archivo)
for parquet_file in all_files:
    print(f"📥 Procesando archivo: {parquet_file}")
    df = spark.read.parquet(parquet_file)

    # 🧼 Normaliza nombres de columnas
    for old_col in df.columns:
        new_col = old_col.lower().replace(" ", "_")
        df = df.withColumnRenamed(old_col, new_col)

    # 🧽 Limpieza básica
    df = df.dropDuplicates()

    # ✅ Tipado por columnas (ajústalo según tus datos)
    for column_name in df.columns:
        if "date" in column_name or "datetime" in column_name:
            df = df.withColumn(column_name, col(column_name).cast("timestamp"))
        elif "id" in column_name:
            df = df.withColumn(column_name, col(column_name).cast("string"))
        elif "amount" in column_name or "fare" in column_name:
            df = df.withColumn(column_name, col(column_name).cast("double"))

    # 📝 Extrae año y mes si existen columnas de fecha
    if "tpep_pickup_datetime" in df.columns:
        df = df.withColumn("year", col("tpep_pickup_datetime").cast("date").substr(1, 4)) \
               .withColumn("month", col("tpep_pickup_datetime").cast("date").substr(6, 2))
    elif "pickup_datetime" in df.columns:
        df = df.withColumn("year", col("pickup_datetime").cast("date").substr(1, 4)) \
               .withColumn("month", col("pickup_datetime").cast("date").substr(6, 2))

    # 💾 Escribe particionado por año y mes si existen, o en modo normal
    if "year" in df.columns and "month" in df.columns:
        print(f"💾 Guardando particionado por año/mes en: {output_path}")
        df.write.mode("append").partitionBy("year", "month").parquet(output_path)
    else:
        print(f"💾 Guardando sin particionar en: {output_path}")
        df.write.mode("append").parquet(output_path)

print("✅ Procesamiento finalizado.")
spark.stop()
