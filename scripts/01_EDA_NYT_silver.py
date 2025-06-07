import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from pathlib import Path

# === Cargar un muestreo desde parquet ===
silver_path = Path("/data/silver/Taxi_NYC/yellow_tripdata")

# Cargar solo un año-mes representativo
sample_file = list(silver_path.glob("year=2022/month=07/*.parquet"))[0]
df = pd.read_parquet(sample_file)

print("\n=== SHAPE ===")
print(df.shape)

print("\n=== TIPOS DE DATOS ===")
print(df.dtypes)

print("\n=== NULOS ===")
print(df.isnull().sum()[df.isnull().sum() > 0])

print("\n=== VALORES CERO O NEGATIVOS EN COLUMNAS CLAVE ===")
for col in ["passenger_count", "trip_distance", "fare_amount"]:
    print(f"{col} <= 0:", (df[col] <= 0).sum())

print("\n=== COLUMNAS CON ÚNICO VALOR ===")
print([col for col in df.columns if df[col].nunique() == 1])

print("\n=== DISTRIBUCIÓN DE TIP AMOUNT ===")
print(df["tip_amount"].describe())

# === Crear nuevas columnas ===
df["trip_duration_min"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60
df["tip_ratio"] = df["tip_amount"] / df["fare_amount"]
df["has_tip"] = (df["tip_amount"] > 0).astype(int)

# === Ver relaciones ===
print("\n=== CORRELACIÓN ENTRE VARIABLES NUMÉRICAS ===")
print(df.corr(numeric_only=True)[["fare_amount", "tip_amount", "trip_duration_min"]])

# === Histograma para ver outliers ===
sns.histplot(df["trip_distance"], bins=50)
plt.title("Distribución de trip_distance")
plt.savefig("plots/trip_distance.png")
plt.close()

sns.histplot(df["fare_amount"], bins=50)
plt.title("Distribución de fare_amount")
plt.savefig("plots/fare_amount.png")
plt.close()

sns.histplot(df["tip_ratio"], bins=50)
plt.title("Distribución de tip_ratio")
plt.savefig("plots/tip_ratio.png")
plt.close()

# === Idea para clasificación: ¿dejó propina? ===
sns.countplot(data=df, x="has_tip")
plt.title("¿Se dejó propina?")
plt.savefig("plots/tip_given.png")
plt.close()
