import os
import csv
from datetime import datetime

class DownloadLogger:
    def __init__(self, log_path="data/logs/download_log.csv"):
        self.log_path = log_path
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        if not os.path.exists(log_path):
            with open(log_path, mode='w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "timestamp", "year", "month", "type", "filename",
                    "status", "message", "duration_seconds", "file_size_mb""timestamp", "year", "month", "type", "filename", "status", "message", "duration_seconds"
                ])

    def log(self, year, month, file_type, file_name, status, message, duration_seconds=None, file_size_mb=None):
        with open(self.log_path, mode='a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.utcnow().isoformat(),
                year,
                month,
                file_type,
                file_name,
                status,
                message,
                round(duration_seconds, 2) if duration_seconds is not None else "",
                round(file_size_mb, 2) if file_size_mb else ""
            ])

    def log_merge(self, tipo, year_or_all, file_path):
        file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
        with open(self.log_path, mode='a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                datetime.utcnow().isoformat(),
                year_or_all,
                "",  # mes vacío
                tipo,
                os.path.basename(file_path),
                "MERGED",
                "Agregado de archivos",
                "",  # duración
                round(file_size, 2)
            ])




