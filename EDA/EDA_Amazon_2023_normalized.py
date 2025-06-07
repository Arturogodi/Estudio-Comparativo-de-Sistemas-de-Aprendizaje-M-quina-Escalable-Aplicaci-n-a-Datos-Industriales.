import pandas as pd
import json
from tabulate import tabulate

# Cargar dataset
df = pd.read_parquet("data/bronze/amazon2023/reviews_Books.parquet")

# Elegir una fila (puedes cambiar el índice)
fila = df.iloc[0]

# Recoger datos en forma de tuplas (clave, valor)
datos = []

for col, val in fila.items():
    if isinstance(val, dict):
        for subkey, subval in val.items():
            datos.append((f"{col}.{subkey}", str(subval)))
    else:
        datos.append((col, str(val)))

# Construir tabla Markdown
markdown = "| Campo | Valor |\n|-------|-------|\n"
for campo, valor in datos:
    # Escapar pipes dentro de valores
    valor = valor.replace("|", "\\|")
    markdown += f"| {campo} | {valor} |\n"

# Guardar a archivo
with open("fila_ejemplo_review.md", "w", encoding="utf-8") as f:
    f.write(markdown)

print("✅ Archivo 'fila_ejemplo.md' generado correctamente.")