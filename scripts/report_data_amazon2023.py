import os
import pandas as pd

def summarize_parquet(path):
    try:
        df = pd.read_parquet(path)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        size_str = f"{size_mb:.1f} MB" if size_mb < 1024 else f"{size_mb/1024:.1f} GB"
        n_rows, n_cols = df.shape
        columns = ", ".join(df.columns[:5]) + ("..." if df.shape[1] > 5 else "")
        n_nulls = int(df.isnull().sum().sum())
        return {
            "Archivo": os.path.basename(path),
            "Tamaño": size_str,
            "Filas": f"{n_rows:,}",
            "Columnas": n_cols,
            "Columnas principales": columns,
            "Nulos totales": f"{n_nulls:,}"
        }
    except Exception as e:
        return {
            "Archivo": os.path.basename(path),
            "Tamaño": "ERROR",
            "Filas": "ERROR",
            "Columnas": "ERROR",
            "Columnas principales": str(e),
            "Nulos totales": "ERROR"
        }

def main():
    base_dir = "data/input/amazon2023"
    report_path = "report_amazon.md"

    files = sorted([f for f in os.listdir(base_dir) if f.endswith(".parquet")])
    rows = []

    for f in files:
        info = summarize_parquet(os.path.join(base_dir, f))
        rows.append(info)

    # Crear el markdown
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# Informe Exploratorio — Dataset Amazon Reviews 2023\n\n")
        f.write("A continuación se presenta un resumen de los archivos `.parquet` procesados desde el directorio `data/input/amazon2023`.\n\n")
        f.write("| Archivo | Tamaño | Filas | Columnas | Columnas principales | Nulos totales |\n")
        f.write("|---------|--------|--------|----------|-----------------------|----------------|\n")
        for r in rows:
            f.write(f"| {r['Archivo']} | {r['Tamaño']} | {r['Filas']} | {r['Columnas']} | {r['Columnas principales']} | {r['Nulos totales']} |\n")

    print(f"✅ Reporte generado en: {report_path}")

if __name__ == "__main__":
    main()
