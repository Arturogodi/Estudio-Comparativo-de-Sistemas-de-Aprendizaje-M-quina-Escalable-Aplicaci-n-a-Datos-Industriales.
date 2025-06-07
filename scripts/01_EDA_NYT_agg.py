import os
import pandas as pd

# Ruta principal
root_path = "data/bronze/Taxi_NYC"

# Datasets a analizar
datasets = ["fhv_tripdata", "green_tripdata", "yellow_tripdata"]

# Lista para almacenar resultados por año
summary_by_year = []

# Analizamos cada año del 2020 al 2024
for year in range(2020, 2025):
    year_path = os.path.join(root_path, str(year))

    for dataset in datasets:
        total_rows = 0
        total_cols = 0
        total_size_bytes = 0
        monthly_counts = 0

        for month in range(1, 13):
            file_name = f"{dataset}_{year}-{month:02d}.parquet"
            file_path = os.path.join(year_path, file_name)

            if os.path.exists(file_path):
                try:
                    df = pd.read_parquet(file_path)
                    file_size = os.path.getsize(file_path)

                    total_size_bytes += file_size
                    total_rows += len(df)
                    total_cols = max(total_cols, len(df.columns))
                    monthly_counts += 1
                except Exception as e:
                    print(f"❌ Error leyendo {file_name}: {e}")
            else:
                print(f"⚠️ No encontrado: {file_path}")

        summary_by_year.append({
            "año": year,
            "dataset": dataset,
            "archivos_encontrados": monthly_counts,
            "total_filas": total_rows,
            "total_columnas": total_cols,
            "tamaño_total_MB": round(total_size_bytes / (1024 * 1024), 2)
        })

# Crear DataFrame de resumen anual
summary_df_year = pd.DataFrame(summary_by_year)
print("\n=== RESUMEN POR AÑO ===")
print(summary_df_year)

# Crear resumen total acumulado por dataset
summary_df_total = summary_df_year.groupby("dataset").agg({
    "archivos_encontrados": "sum",
    "total_filas": "sum",
    "total_columnas": "max",  # máximo número de columnas encontradas en algún archivo
    "tamaño_total_MB": "sum"
}).reset_index()

print("\n=== RESUMEN TOTAL ACUMULADO POR DATASET ===")
print(summary_df_total)
