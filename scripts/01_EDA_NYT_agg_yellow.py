import os
import pandas as pd
import matplotlib.pyplot as plt

# Configura matplotlib para entornos que no tienen display (opcional en notebooks)
plt.switch_backend('Agg')

# Ruta base
root_path = "data/bronze/Taxi_NYC"
years = range(2020, 2025)
yellow_data = []

# Carga de archivos yellow_tripdata
for year in years:
    for month in range(1, 13):
        file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        file_path = os.path.join(root_path, str(year), file_name)

        if os.path.exists(file_path):
            try:
                df = pd.read_parquet(file_path)
                df["year"] = year
                df["month"] = month
                yellow_data.append(df)
            except Exception as e:
                print(f"❌ Error leyendo {file_name}: {e}")
        else:
            print(f"⚠️ No encontrado: {file_path}")

# Concatenar y analizar
if yellow_data:
    df_yellow = pd.concat(yellow_data, ignore_index=True)

    # Análisis de valores nulos
    null_counts = df_yellow.isnull().sum().sort_values(ascending=False)
    null_percent = (null_counts / len(df_yellow)) * 100

    null_df = pd.DataFrame({
        "columna": null_counts.index,
        "nulos": null_counts.values,
        "porcentaje": null_percent.round(2)
    })

    print("\n=== VALORES NULOS ===")
    print(null_df)

    # Tipos de datos
    dtypes_df = pd.DataFrame({
        "columna": df_yellow.columns,
        "tipo_dato": df_yellow.dtypes.values
    })

    print("\n=== TIPOS DE DATOS ===")
    print(dtypes_df)

    # Gráfico: distancia promedio de viaje por año
    avg_distance = df_yellow.groupby("year")["trip_distance"].mean()

    plt.figure(figsize=(8, 5))
    avg_distance.plot(kind="bar")
    plt.title("Distancia Promedio de Viaje por Año (Yellow Taxis)")
    plt.xlabel("Año")
    plt.ylabel("Distancia Promedio (millas)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("distancia_promedio_yellow.png")

    print("\n✅ Gráfico guardado como: distancia_promedio_yellow.png")

else:
    print("❌ No se encontraron archivos yellow_tripdata válidos.")
