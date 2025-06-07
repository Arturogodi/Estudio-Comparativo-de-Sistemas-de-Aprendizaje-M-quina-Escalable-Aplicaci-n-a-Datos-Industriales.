from collections import defaultdict
import re

# Ruta al archivo resumen
file_path = "/data/logs/parquet_schema_summary.txt"

# Estructura para guardar tipos por columna
column_type_map = defaultdict(lambda: defaultdict(int))

with open(file_path, "r", encoding="utf-8") as f:
    lines = f.readlines()

for line in lines:
    if line.startswith("Columnas (5):"):
        columnas_raw = line.split(":", 1)[1].strip()
        columnas = [col.strip() for col in columnas_raw.split(",")]
        for col_def in columnas:
            if ": " in col_def:
                name, dtype = col_def.split(": ")
                column_type_map[name][dtype] += 1

# Mostrar resumen
print("\n📊 Resumen de tipos por columna:")
for col_name, types in column_type_map.items():
    print(f"\n🔹 Columna: {col_name}")
    for dtype, count in types.items():
        print(f"   - {dtype}: {count} archivo(s)")

# Detectar columnas con más de un tipo
print("\n⚠️ Columnas con tipos inconsistentes:")
inconsistentes = [col for col, types in column_type_map.items() if len(types) > 1]
for col in inconsistentes:
    print(f"  - {col}: {list(column_type_map[col].keys())}")
