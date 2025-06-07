import os
import pandas as pd

# Ruta base
base_path = "data/bronze/Taxi_NYC/2020"

# Tipos de datasets
datasets = ["fhv_tripdata", "green_tripdata", "yellow_tripdata"]

# Función para cargar y analizar
def analizar_dataset(dataset, year=2020):
    print(f"\n=== Analizando {dataset} del año {year} ===")
    for month in range(1, 13):
        file_name = f"{dataset}_{year}-{month:02d}.parquet"
        file_path = os.path.join(base_path, file_name)
        
        if os.path.exists(file_path):
            print(f"\n📂 Cargando archivo: {file_name}")
            try:
                df = pd.read_parquet(file_path)

                print(f"✅ Filas: {len(df)}")
                print(f"📊 Columnas: {list(df.columns)}")
                print(df.dtypes)
                print("📈 Estadísticas generales (numéricas):")
                print(df.describe(include='all').transpose().head(5))
            except Exception as e:
                print(f"❌ Error al leer {file_name}: {e}")
        else:
            print(f"⚠️ Archivo no encontrado: {file_path}")

# Ejecutar análisis para cada dataset
for dataset in datasets:
    analizar_dataset(dataset)
