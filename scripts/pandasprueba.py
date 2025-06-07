from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
import os
import re

# 🔧 Rutas locales usando pathlib
base_dir = Path(__file__).resolve().parents[1]  # sube al proyecto
dataset_path = base_dir / "data" / "silver" / "consolidado"
output_dir = base_dir / "data" / "silver" / "Taxi_NYC_test" / "yellow_tripdata_pandas_cleaned"

print(f"📥 Leyendo conjunto de archivos desde: {dataset_path}")

# ✅ Leer dataset parquet (multi-part)
dataset = ds.dataset(str(dataset_path), format="parquet")
df = dataset.to_table().to_pandas()

# 🧼 Normalizar nombres de columnas
df.columns = [re.sub(r"\s+", "_", col.lower()) for col in df.columns]

# 🧽 Eliminar duplicados exactos
df.drop_duplicates(inplace=True)

# 🎯 Cast por tipo (simplificado)
for col in df.columns:
    if "datetime" in col or "date" in col:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    elif "amount" in col or "fare" in col or "distance" in col or "surcharge" in col:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    elif "id" in col:
        df[col] = df[col].astype("Int64")

# 📆 Extraer año y mes
if "tpep_pickup_datetime" in df.columns:
    df["year"] = df["tpep_pickup_datetime"].dt.year
    df["month"] = df["tpep_pickup_datetime"].dt.month
else:
    raise ValueError("❌ No se encontró la columna 'tpep_pickup_datetime'.")

# 💾 Guardar CSV por año y mes
print("💾 Guardando CSVs particionados por año y mes...")
for (year, month), group in df.groupby(["year", "month"]):
    out_path = output_dir / f"year={year}" / f"month={month}"
    out_path.mkdir(parents=True, exist_ok=True)
    out_file = out_path / "yellow_tripdata.csv"
    print(f"📝 Escribiendo: {out_file}")
    group.to_csv(out_file, index=False)

print("✅ Proceso completado.")
