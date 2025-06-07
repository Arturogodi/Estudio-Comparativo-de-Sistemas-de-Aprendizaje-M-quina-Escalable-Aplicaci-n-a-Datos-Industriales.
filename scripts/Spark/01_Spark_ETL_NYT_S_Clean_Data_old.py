import yaml

with open("configs/benchmark_config.yaml", "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)
    print("✅ YAML cargado:", type(data))
    print("🧪 Claves disponibles:", list(data.keys()))
    print("📦 Contenido de C1:", data.get("C1"))


# scripts/Spark/01_Spark_ETL_NYT_S_Clean_Data.py

import sys
from pathlib import Path

# === Añadir el path raíz del proyecto para importar helpers ===
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# === Imports principales ===
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, coalesce
import yaml
from helpers.benchmark_logger import BenchmarkLogger

# === Cargar configuración de benchmark (por ejemplo, C1) ===
with open("configs/benchmark_config.yaml") as f:
    config = yaml.safe_load(f)["C1"]

logger = BenchmarkLogger(config=config, log_dir="logs", log_file="spark_etl_log.csv")

# === Inicializar Spark ===
logger.start("Inicializando SparkSession")
spark = SparkSession.builder \
    .appName("ETL_Bronze_to_Silver_Taxi_Yellow") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
logger.stop_and_log(record_count=0, operation_name="init_spark")

# === Leer parquet de múltiples años ===
input_path = "/data/bronze/Taxi_NYC/"
output_path = "/data/silver/Taxi_NYC/yellow_tripdata"

logger.start("Leyendo parquet de yellow_tripdata")
df = spark.read.option("mergeSchema", True).parquet(
    f"{input_path}{{2020,2021,2022,2023,2024}}/yellow_tripdata_*.parquet"
)
logger.stop_and_log(record_count=df.count(), operation_name="read_parquet", source_file="yellow_tripdata_2020-2024")

# === Renombrar columnas a snake_case ===
logger.start("Normalizando nombres de columnas")
for old_col in df.columns:
    new_col = regexp_replace(lower(old_col), r"\s+", "_")
    df = df.withColumnRenamed(old_col, new_col)
logger.stop_and_log(record_count=df.count(), operation_name="normalize_columns")

# === Unificar columnas duplicadas ===
if "airport_fee" in df.columns and "Airport_fee" in df.columns:
    logger.start("Unificando columnas airport_fee y Airport_fee")
    df = df.withColumn("airport_fee", coalesce(col("airport_fee"), col("Airport_fee"))) \
           .drop("Airport_fee")
    logger.stop_and_log(record_count=df.count(), operation_name="unify_airport_fee")

# === Eliminar duplicados ===
logger.start("Eliminando duplicados exactos")
df = df.dropDuplicates()
logger.stop_and_log(record_count=df.count(), operation_name="drop_duplicates")

# === Escritura particionada por año y mes ===
logger.start("Escribiendo a silver partitionado por año y mes")
df.write.mode("overwrite").partitionBy("year", "month").parquet(output_path)
logger.stop_and_log(record_count=df.count(), operation_name="write_parquet", output_path=output_path)

# === Finalizar Spark ===
spark.stop()
