import os
import requests

def download_tlc_data(year: int, base_path: str = "data/input"):
    year_path = os.path.join(base_path, str(year))
    os.makedirs(year_path, exist_ok=True)

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    file_types = [
        "yellow_tripdata",
        "green_tripdata",
        "fhv_tripdata",
    ]

    for file_type in file_types:
        for month in range(1, 13):
            file_name = f"{file_type}_{year}-{month:02d}.parquet"
            url = f"{base_url}/{file_name}"
            dest_path = os.path.join(year_path, file_name)

            print(f"Descargando: {file_name}...")
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    with open(dest_path, "wb") as f:
                        f.write(response.content)
                    print(f"✔ Guardado en: {dest_path}")
                else:
                    print(f"✘ No encontrado: {url} (status {response.status_code})")
            except Exception as e:
                print(f"⚠ Error con {url}: {e}")


if __name__ == "__main__":
    download_tlc_data(2020)


