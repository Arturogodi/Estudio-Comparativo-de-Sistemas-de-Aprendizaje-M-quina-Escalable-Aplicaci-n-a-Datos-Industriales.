import os
import pandas as pd
from utils.logging import DownloadLogger

def merge_by_type_and_year(base_input="data/input", base_output="data/modification"):
    os.makedirs(base_output, exist_ok=True)
    logger = DownloadLogger(log_path="data/logs/merge_log.csv")

    years = sorted(os.listdir(base_input))  # ["2020", "2021", ...]
    file_types = ["yellow", "green", "fhv"]

    yearly_paths = {tipo: [] for tipo in file_types}

    for year in years:
        year_path = os.path.join(base_input, year)
        if not os.path.isdir(year_path):
            continue

        for tipo in file_types:
            files = [
                os.path.join(year_path, f) for f in os.listdir(year_path)
                if f.startswith(tipo) and f.endswith(".parquet")
            ]
            if not files:
                continue

            # Concatenar por tipo y año
            df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
            output_path = os.path.join(base_output, f"{tipo}_{year}.parquet")
            df.to_parquet(output_path, index=False)
            logger.log_merge(tipo, year, output_path)
            yearly_paths[tipo].append(output_path)

    # Unir todos los años por tipo
    for tipo in file_types:
        all_dfs = [pd.read_parquet(p) for p in yearly_paths[tipo]]
        df_all = pd.concat(all_dfs, ignore_index=True)
        output_all = os.path.join(base_output, f"{tipo}_all.parquet")
        df_all.to_parquet(output_all, index=False)
        logger.log_merge(tipo, "ALL", output_all)

if __name__ == "__main__":
    merge_by_type_and_year()
