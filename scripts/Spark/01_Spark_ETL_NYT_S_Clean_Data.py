import sys
from pathlib import Path
import yaml
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, year, month
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType

# Añadir path raíz del proyecto
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# Importar el logger
sys.path.append("/")
from helpers.benchmark_logger import BenchmarkLogger

# Cargar configuración
with open("configs/benchmark_config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)["C1"]

logger = BenchmarkLogger(config=config, log_dir="logs", log_file="spark_etl_log.csv")

# Inicializar Spark
logger.start("Inicializando SparkSession")
spark = SparkSession.builder \
    .appName("ETL_Bronze_to_Silver_Taxi_Yellow") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
logger.stop_and_log(record_count=0, operation_name="init_spark")

# Rutas
bronze_path = "/data/bronze/Taxi_NYC/"
temp_clean_path = "/data/silver/tmp_cleaned/"
final_output_path = "/data/silver/Taxi_NYC/yellow_tripdata"

# Paso 1: Leer sin schema para poder reparar los archivos
logger.start("Leyendo parquet de yellow_tripdata sin esquema para limpieza")
df_dirty = spark.read.parquet(f"{bronze_path}{{2020,2021,2022,2023,2024}}/yellow_tripdata_*.parquet")

# Castear columnas manualmente
df_clean = df_dirty \
    .withColumn("VendorID", col("VendorID").cast(LongType())) \
    .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast(TimestampType())) \
    .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast(TimestampType())) \
    .withColumn("passenger_count", col("passenger_count").cast(DoubleType())) \
    .withColumn("trip_distance", col("trip_distance").cast(DoubleType())) \
    .withColumn("RatecodeID", col("RatecodeID").cast(DoubleType())) \
    .withColumn("store_and_fwd_flag", col("store_and_fwd_flag").cast(StringType())) \
    .withColumn("PULocationID", col("PULocationID").cast(LongType())) \
    .withColumn("DOLocationID", col("DOLocationID").cast(LongType())) \
    .withColumn("payment_type", col("payment_type").cast(LongType())) \
    .withColumn("fare_amount", col("fare_amount").cast(DoubleType())) \
    .withColumn("extra", col("extra").cast(DoubleType())) \
    .withColumn("mta_tax", col("mta_tax").cast(DoubleType())) \
    .withColumn("tip_amount", col("tip_amount").cast(DoubleType())) \
    .withColumn("tolls_amount", col("tolls_amount").cast(DoubleType())) \
    .withColumn("improvement_surcharge", col("improvement_surcharge").cast(DoubleType())) \
    .withColumn("total_amount", col("total_amount").cast(DoubleType())) \
    .withColumn("congestion_surcharge", col("congestion_surcharge").cast(DoubleType())) \
    .withColumn("airport_fee", col("airport_fee").cast(DoubleType()))
logger.stop_and_log(record_count=df_clean.count(), operation_name="cast_and_clean")

# Paso 2: Guardar temporalmente en silver limpio
logger.start("Escribiendo parquet limpio temporal")
df_clean.write.mode("overwrite").parquet(temp_clean_path)
logger.stop_and_log(record_count=df_clean.count(), operation_name="write_tmp_cleaned")

# Paso 3: Definir esquema explícito unificado para lectura segura
schema = StructType([
    StructField("VendorID", LongType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", LongType(), True),
    StructField("DOLocationID", LongType(), True),
    StructField("payment_type", LongType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True)
])

# Paso 4: Leer desde tmp_cleaned con schema
logger.start("Releyendo parquet limpio con schema")
df = spark.read.schema(schema).parquet(temp_clean_path)
logger.stop_and_log(record_count=df.count(), operation_name="read_cleaned_parquet")

# Normalizar nombres de columnas
logger.start("Normalizando nombres de columnas")
for old_col in df.columns:
    new_col = re.sub(r"\s+", "_", old_col.lower())
    df = df.withColumnRenamed(old_col, new_col)
logger.stop_and_log(record_count=df.count(), operation_name="normalize_columns")

# Unificar columnas duplicadas si existen
if "airport_fee" in df.columns and "Airport_fee" in df.columns:
    logger.start("Unificando columnas airport_fee y Airport_fee")
    df = df.withColumn("airport_fee", coalesce(col("airport_fee"), col("Airport_fee"))) \
           .drop("Airport_fee")
    logger.stop_and_log(record_count=df.count(), operation_name="unify_airport_fee")

# Eliminar duplicados exactos
logger.start("Eliminando duplicados exactos")
df = df.dropDuplicates()
logger.stop_and_log(record_count=df.count(), operation_name="drop_duplicates")

# Añadir columnas de año y mes para particionado
df = df.withColumn("year", year("tpep_pickup_datetime")) \
       .withColumn("month", month("tpep_pickup_datetime"))

# Escritura particionada final
logger.start("Escribiendo a silver partitionado por año y mes")
df.write.mode("overwrite").partitionBy("year", "month").parquet(final_output_path)
logger.stop_and_log(record_count=df.count(), operation_name="write_parquet")

# Finalizar Spark
spark.stop()
