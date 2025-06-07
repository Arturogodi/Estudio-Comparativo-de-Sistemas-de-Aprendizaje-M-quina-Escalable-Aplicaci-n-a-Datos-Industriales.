import os
import requests
import time

from utils.logging import DownloadLogger


def download_tlc_data(year: int, base_path: str = "data/input", logger: DownloadLogger = None):
    year_path = os.path.join(base_path, str(year))
    os.makedirs(year_path, exist_ok=True)

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    file_types = ["yellow_tripdata", "green_tripdata", "fhv_tripdata"]

    if logger is None:
        logger = DownloadLogger()

    for file_type in file_types:
        for month in range(1, 13):
            file_name = f"{file_type}_{year}-{month:02d}.parquet"
            url = f"{base_url}/{file_name}"
            dest_path = os.path.join(year_path, file_name)

            if os.path.exists(dest_path):
                logger.log(year, month, file_type, file_name, "SKIPPED", "Archivo ya existe")
                continue

            try:
                start_time = time.time()
                response = requests.get(url)
                duration = time.time() - start_time

                if response.status_code == 200:
                    with open(dest_path, "wb") as f:
                        f.write(response.content)
                    logger.log(year, month, file_type, file_name, "OK", "Descargado correctamente", duration)
                else:
                    logger.log(year, month, file_type, file_name, "FAIL", f"Status code {response.status_code}")
            except Exception as e:
                logger.log(year, month, file_type, file_name, "FAIL", str(e))
