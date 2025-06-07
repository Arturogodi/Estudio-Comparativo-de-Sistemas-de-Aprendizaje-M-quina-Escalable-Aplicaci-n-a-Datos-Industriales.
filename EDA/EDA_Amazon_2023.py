import pandas as pd
import os

# Paths
meta_path = "data/bronze/amazon2023/meta_Books.parquet"
reviews_path = "data/bronze/amazon2023/reviews_Books.parquet"
os.makedirs("EDA", exist_ok=True)

# Cargar datasets
print("Cargando archivos...")
df_meta = pd.read_parquet(meta_path)
df_reviews = pd.read_parquet(reviews_path)

# Tamaños y columnas
print("\nMeta:")
print(df_meta.shape)
print(df_meta.columns)

print("\nReviews:")
print(df_reviews.shape)
print(df_reviews.columns)

# Nulos
print("\nNulos (meta):")
print(df_meta.isnull().sum())

print("\nNulos (reviews):")
print(df_reviews.isnull().sum())

# Tipos y resumen estadístico
print("\nTipos:")
print(df_meta.dtypes)
print(df_reviews.dtypes)

print("\nResumen meta:")
print(df_meta.describe(include="all").T)

print("\nResumen reviews:")
print(df_reviews.describe(include="all").T)

# Unicidad y claves
if "asin" in df_meta.columns and "asin" in df_reviews.columns:
    print("\nProductos únicos (meta):", df_meta["asin"].nunique())
    print("Productos únicos con reviews:", df_reviews["asin"].nunique())

    # Merge para verificar alineamiento
    merged = df_reviews.merge(df_meta[["asin"]], on="asin", how="left", indicator=True)
    no_meta = merged[merged["_merge"] == "left_only"]
    print("Reviews sin metadatos:", no_meta.shape[0])

    no_reviews = df_meta[~df_meta["asin"].isin(df_reviews["asin"])]
    print("Productos sin reviews:", no_reviews.shape[0])

    # Reviews por producto
    reviews_per_product = df_reviews["asin"].value_counts()
    reviews_per_product.describe().to_csv("EDA/reviews_per_product_stats.csv")

# Guardar resúmenes
df_meta.describe(include="all").T.to_csv("EDA/meta_books_summary.csv")
df_reviews.describe(include="all").T.to_csv("EDA/reviews_books_summary.csv")
