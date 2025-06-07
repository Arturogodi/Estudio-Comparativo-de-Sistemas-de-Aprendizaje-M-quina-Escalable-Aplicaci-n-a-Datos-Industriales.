import pandas as pd

def explore_file(file_path, label):
    print(f"\n📂 {label.upper()} DATA — {file_path}")
    try:
        df = pd.read_parquet(file_path)

        print("→ Columnas:", df.columns.tolist())
        print("→ Dimensiones:", df.shape)
        print("→ Primeras filas:\n", df.head(3))

        try:
            print("→ Estadísticas descriptivas:")
            print(df.describe(include='all').T)
        except Exception as e:
            print(f"⚠ Error al calcular estadísticas: {e}")

    except Exception as e:
        print(f"⚠ Error al leer {label}: {e}")

def main():
    base_path = "data/input/2020"
    files = {
        "yellow": f"{base_path}/yellow_tripdata_2020-01.parquet",
        "green": f"{base_path}/green_tripdata_2020-01.parquet",
        "fhv": f"{base_path}/fhv_tripdata_2020-01.parquet",
    }

    for tipo, path in files.items():
        explore_file(path, tipo)

if __name__ == "__main__":
    main()
