# helpers/benchmark_logger.py

import time
import os
import csv
from datetime import datetime
import psutil
from helpers.spinner import Spinner


class BenchmarkLogger:
    def __init__(self, config: dict, log_dir="logs", log_file="benchmark_log.csv"):
        self.config = config  # Diccionario con los parámetros de configuración (C1, C2, etc.)
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self.log_path = os.path.join(log_dir, log_file)
        self.start_time = None
        self.spinner = None
        self.cpu_usage = []
        self.mem_usage = []
        self.process = psutil.Process(os.getpid())

        if not os.path.exists(self.log_path):
            with open(self.log_path, mode='w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
                writer.writeheader()

    def _get_fieldnames(self):
        return [
            "timestamp", "step_name", "operation_name", "framework", "operation_type",
            "dataset_name", "record_count", "duration_sec",
            "cpu_usage_mean", "mem_usage_mb",
            "num_workers", "cores_per_worker", "ram_per_worker_gb",
            "notes"
        ]

    def start(self, message="Procesando..."):
        self.start_time = time.time()
        self.cpu_usage.clear()
        self.mem_usage.clear()
        self.spinner = Spinner(message=message)
        self.spinner.start()

    def track(self):
        self.cpu_usage.append(psutil.cpu_percent(interval=None))
        self.mem_usage.append(self.process.memory_info().rss / (1024 * 1024))  # MB

    def stop_and_log(self, record_count, operation_name="unspecified"):
        if self.spinner:
            self.spinner.stop()

        end_time = time.time()
        duration = round(end_time - self.start_time, 2)
        cpu_mean = round(sum(self.cpu_usage) / len(self.cpu_usage), 2) if self.cpu_usage else 0.0
        mem_avg = round(sum(self.mem_usage) / len(self.mem_usage), 2) if self.mem_usage else 0.0

        row = {
            "timestamp": datetime.now().isoformat(),
            "step_name": self.config["step_name"],
            "operation_name": operation_name,
            "framework": self.config["framework"],
            "operation_type": self.config["operation_type"],
            "dataset_name": self.config["dataset_name"],
            "record_count": record_count,
            "duration_sec": duration,
            "cpu_usage_mean": cpu_mean,
            "mem_usage_mb": mem_avg,
            "num_workers": self.config["num_workers"],
            "cores_per_worker": self.config["cores_per_worker"],
            "ram_per_worker_gb": self.config["ram_per_worker_gb"],
            "notes": self.config.get("notes", "")
        }

        with open(self.log_path, mode='a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self._get_fieldnames())
            writer.writerow(row)

        print(f"✅ LOGGED: {operation_name} | {duration}s | {cpu_mean}% CPU | {mem_avg}MB RAM")
