import pandas as pd
from tabulate import tabulate

# Cargar el Parquet combinado
df = pd.read_parquet("data/silver/sample_join_5rows.parquet")

# Seleccionar una fila (puedes cambiar el índice)
fila = df.iloc[0]

# Preparar contenido como lista (clave, valor)
datos = []
for col, val in fila.items():
    if isinstance(val, dict):
        for subkey, subval in val.items():
            datos.append((f"{col}.{subkey}", str(subval)))
    else:
        datos.append((col, str(val)))

# Mostrar como tabla en consola
print(tabulate(datos, headers=["Campo", "Valor"], tablefmt="fancy_grid"))
