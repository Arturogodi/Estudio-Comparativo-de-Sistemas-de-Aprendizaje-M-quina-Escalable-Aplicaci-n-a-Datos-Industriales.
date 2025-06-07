import ray
from ray import data as ray_data
from pathlib import Path
import pandas as pd
import yaml

from helpers.benchmark_logger import BenchmarkLogger

# === Cargar configuración YAML ===
with open("configs/benchmark_config.yaml") as f:
    config = yaml.safe_load(f)["C1"]  # Cambiar a C2/C3/... sin tocar el código

logger = BenchmarkLogger(config=config, log_dir="logs", log_file="ray_etl_log.csv")

# === Inicializar Ray ===
logger.start("Inicializando Ray")
ray.init(ignore_reinit_error=True)
logger.stop_and_log(record_count=0, operation_name="init_ray")

# === Generar lista de archivos parquet ===
logger.start("Listando archivos parquet")
base_path = Path("/data/bronze/Taxi_NYC")
parquet_files = list(base_path.glob("20*/yellow_tripdata_*.parquet"))
logger.stop_and_log(record_count=len(parquet_files), operation_name="list_parquet_files")

# === Leer datasets con Ray ===
logger.start("Leyendo datos con Ray")
ds = ray_data.read_parquet([str(p) for p in parquet_files])
logger.stop_and_log(record_count=ds.count(), operation_name="read_parquet", source_file="yellow_tripdata_2020-2024")

# === Normalizar nombres de columnas ===
logger.start("Normalizando nombres de columnas")
ds = ds.rename_columns({col: col.strip().lower().replace(" ", "_") for col in ds.schema().names})
logger.stop_and_log(record_count=ds.count(), operation_name="normalize_columns")

# === Fusionar columnas duplicadas (ej. airport_fee vs Airport_fee) ===
if "airport_fee" in ds.schema().names and "Airport_fee" in ds.schema().names:
    logger.start("Unificando columnas duplicadas")

    def unify_airport_fee(batch: pd.DataFrame) -> pd.DataFrame:
        batch["airport_fee"] = batch["airport_fee"].combine_first(batch["Airport_fee"])
        batch.drop(columns=["Airport_fee"], inplace=True)
        return batch

    ds = ds.map_batches(unify_airport_fee)
    logger.stop_and_log(record_count=ds.count(), operation_name="unify_airport_fee")

# === Eliminar duplicados ===
logger.start("Eliminando duplicados")
ds = ds.drop_duplicates()
logger.stop_and_log(record_count=ds.count(), operation_name="drop_duplicates")

# === Escribir resultados en Parquet ===
output_path = "/data/silver/Taxi_NYC/yellow_tripdata"
logger.start("Escribiendo resultados en parquet")
ds.write_parquet(output_path)
logger.stop_and_log(record_count=ds.count(), operation_name="write_parquet", output_path=output_path)

# === Finalizar Ray ===
ray.shutdown()
