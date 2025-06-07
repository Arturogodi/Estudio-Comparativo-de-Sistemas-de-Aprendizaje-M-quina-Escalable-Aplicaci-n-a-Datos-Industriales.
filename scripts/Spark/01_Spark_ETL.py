from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
import re
import os

# Inicializar Spark
spark = SparkSession.builder \
    .appName("NYC_Taxi_Cleaning") \
    .getOrCreate()

# Ruta base de los Parquet
input_path = "/data/bronze/Taxi_NYC/"
output_csv = "/data/silver/NYC_processed/clean01_csv"

# Leer todos los archivos Parquet por año
valid_dfs = []

years = ["2020", "2021", "2022", "2023", "2024"]
for year in years:
    year_path = os.path.join(input_path, year)
    try:
        files = [f for f in os.listdir(year_path) if f.endswith(".parquet")]
    except FileNotFoundError:
        continue

    for file in files:
        full_path = os.path.join(year_path, file)
        try:
            df = spark.read.parquet(full_path)

            # Normalizar columnas
            for old_col in df.columns:
                new_col = re.sub(r"\s+", "_", old_col.lower())
                df = df.withColumnRenamed(old_col, new_col)

            valid_dfs.append(df)

        except Exception as e:
            print(f"⚠️ Error leyendo {full_path}: {e}")
            continue

# Concatenar todos los DataFrames válidos
if valid_dfs:
    final_df = valid_dfs[0]
    for df in valid_dfs[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

    # Escribir en CSV
    final_df.write.mode("overwrite").option("header", "true").csv(output_csv)
    print(f"✅ Escrito correctamente en {output_csv}")
else:
    print("❌ No se pudo leer ningún archivo válido.")
