from tripdata_download import download_tlc_data
from utils.logging import DownloadLogger

logger = DownloadLogger()
for year in range(2020, 2025):
    download_tlc_data(year, logger=logger)
    try:
        print(f"\n{year} Ejecución exitosa.")
    except Exception as e:
        print(f"\n{year} Ejecución fallida: {e}")

