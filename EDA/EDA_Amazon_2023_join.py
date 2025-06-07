import pandas as pd

# Paths
meta_path = "data/bronze/amazon2023/meta_Books.parquet"
reviews_path = "data/bronze/amazon2023/reviews_Books.parquet"

# Leer los datasets
df_meta = pd.read_parquet(meta_path)
df_reviews = pd.read_parquet(reviews_path)

# Asegurarse de que parent_asin esté como string
df_meta['parent_asin'] = df_meta['parent_asin'].astype(str)
df_reviews['parent_asin'] = df_reviews['parent_asin'].astype(str)

# Buscar 5 parent_asin comunes
asin_comunes = set(df_meta['parent_asin']).intersection(df_reviews['parent_asin'])
asin_comunes = list(asin_comunes)[:5]

# Filtrar 5 filas de cada dataset
meta_filtrado = df_meta[df_meta['parent_asin'].isin(asin_comunes)]
reviews_filtrado = df_reviews[df_reviews['parent_asin'].isin(asin_comunes)]

# Hacer el join por parent_asin
df_join = reviews_filtrado.merge(meta_filtrado, on='parent_asin', how='inner')

# Mostrar las columnas planas disponibles
print("✅ Columnas del resultado:")
print(df_join.columns.tolist())

# Mostrar las primeras filas (solo columnas seleccionadas por claridad)
cols_mostrar = ['parent_asin', 'title_x', 'rating', 'verified_purchase', 'main_category', 'title_y', 'price']
df_simplificado = df_join[cols_mostrar].rename(columns={
    'title_x': 'review_title',
    'title_y': 'product_title'
})

print("\n🧾 5 filas combinadas:")
print(df_simplificado.head())

df_simplificado.to_parquet("data/silver/sample_join_5rows.parquet", index=False)
