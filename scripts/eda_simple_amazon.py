import pandas as pd
import os

def analyze_parquet(path):
    print(f"\n📁 Archivo: {os.path.basename(path)}")
    
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"❌ Error al leer el archivo: {e}")
        return
    
    size_mb = os.path.getsize(path) / (1024 * 1024)
    print(f"📦 Tamaño: {size_mb:.2f} MB")
    print(f"🔢 Filas: {df.shape[0]:,} | Columnas: {df.shape[1]}")
    
    print("\n🔍 Tipos de datos:")
    print(df.dtypes)

    print("\n❓ Valores nulos por columna:")
    print(df.isnull().sum())

    print("\n🧪 Primeras 3 filas:")
    print(df.head(3))
    print("-" * 80)

def main():
    base_dir = "data/input/amazon2023"
    files = [f for f in os.listdir(base_dir) if f.endswith(".parquet")]

    for f in sorted(files):
        analyze_parquet(os.path.join(base_dir, f))

if __name__ == "__main__":
    main()
