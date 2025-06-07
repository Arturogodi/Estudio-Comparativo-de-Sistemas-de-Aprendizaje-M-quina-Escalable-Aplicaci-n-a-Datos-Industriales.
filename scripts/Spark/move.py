from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, DoubleType, StringType, TimestampType

spark = SparkSession.builder.appName("ETL_YellowTaxi_StreamWrite").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

input_path = Path("/data/bronze/Taxi_NYC/")
output_path = "/data/silver/consolidado/"

# Tipos deseados por columna
cast_columns = {
    "VendorID": LongType(),
    "tpep_pickup_datetime": TimestampType(),
    "tpep_dropoff_datetime": TimestampType(),
    "passenger_count": DoubleType(),
    "trip_distance": DoubleType(),
    "RatecodeID": DoubleType(),
    "store_and_fwd_flag": StringType(),
    "PULocationID": LongType(),
    "DOLocationID": LongType(),
    "payment_type": LongType(),
    "fare_amount": DoubleType(),
    "extra": DoubleType(),
    "mta_tax": DoubleType(),
    "tip_amount": DoubleType(),
    "tolls_amount": DoubleType(),
    "improvement_surcharge": DoubleType(),
    "total_amount": DoubleType(),
    "congestion_surcharge": DoubleType(),
    "airport_fee": DoubleType()
}

# Recorremos cada archivo
for f in sorted(input_path.glob("*/yellow_tripdata_*.parquet")):
    try:
        print(f"📥 Procesando archivo: {f}")
        df = spark.read.parquet(str(f))

        for col_name, col_type in cast_columns.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, col(col_name).cast(col_type))

        df.write.mode("append").parquet(output_path)
        print(f"✅ Guardado en: {output_path}")

    except Exception as e:
        print(f"❌ Error en archivo {f.name}: {e}")

spark.stop()
