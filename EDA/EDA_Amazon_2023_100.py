import pandas as pd

# Ruta al archivo Parquet
archivo_parquet = 
meta_path = "data/bronze/amazon2023/meta_Books.parquet"
reviews_path = "data/bronze/amazon2023/reviews_Books.parquet"


# Leer solo las primeras 100 filas
df = pd.read_parquet(archivo_parquet)
df_sample = df.head(100)

# Desanidar la columna 'features'
def parse_json_safe(val):
    try:
        if isinstance(val, str):
            return json.loads(val)
        else:
            return val
    except:
        return None

# Aplicar parsing
parsed_features = df_sample['features'].dropna().apply(parse_json_safe)

# Crear DataFrame plano con json_normalize
features_flat = pd.json_normalize(parsed_features)

# Concatenar con el original (sin sobrescribir)
df_combined = pd.concat([df_sample.reset_index(drop=True), features_flat.reset_index(drop=True)], axis=1)

# Ver columnas nuevas generadas
print(df_combined.columns)
print(df_combined.head())
"""

# Mostrar información básica
print("Columnas:")
print(df_sample.columns)

print("\nTipos de datos:")
print(df_sample.dtypes)

print("\nDescripción estadística:")
print(df_sample.describe(include='all'))

print("\nPrimeras 5 filas:")
print(df_sample.head())

# Guardar la muestra si quieres reutilizarla
df_sample.to_csv("sample_100_filas.csv", index=False)
"""