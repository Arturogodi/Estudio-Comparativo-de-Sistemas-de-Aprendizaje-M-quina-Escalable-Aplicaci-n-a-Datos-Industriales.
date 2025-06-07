# helpers/benchmark_logger.py

import os
import csv
import time
from datetime import datetime
import psutil


class BenchmarkLogger:
    def __init__(self, config: dict, log_dir="logs", log_file="benchmark_log.csv"):
        self.config = config
        os.makedirs(log_dir, exist_ok=True)
        self.log_path = os.path.join(log_dir, log_file)
        self.start_time = None
        self.cpu_samples = []
        self.mem_samples = []
        self.process = psutil.Process(os.getpid())

        if not os.path.exists(self.log_path):
            with open(self.log_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=self._fieldnames())
                writer.writeheader()

    def _fieldnames(self):
        return [
            "timestamp", "step", "framework", "operation", "dataset", "records",
            "duration_sec", "cpu_avg_percent", "ram_avg_mb",
            "workers", "cores_per_worker", "ram_per_worker_gb", "notes"
        ]

    def start(self):
        self.start_time = time.perf_counter()
        self.cpu_samples.clear()
        self.mem_samples.clear()

    def track(self):
        self.cpu_samples.append(psutil.cpu_percent(interval=None))
        self.mem_samples.append(self.process.memory_info().rss / (1024 * 1024))

    def stop_and_log(self, operation: str, records: int = 0):
        duration = round(time.perf_counter() - self.start_time, 4)
        cpu_avg = round(sum(self.cpu_samples) / len(self.cpu_samples), 2) if self.cpu_samples else 0.0
        mem_avg = round(sum(self.mem_samples) / len(self.mem_samples), 2) if self.mem_samples else 0.0

        row = {
            "timestamp": datetime.now().isoformat(timespec='seconds'),
            "step": self.config.get("step", ""),
            "framework": self.config.get("framework", ""),
            "operation": operation,
            "dataset": self.config.get("dataset", ""),
            "records": records,
            "duration_sec": duration,
            "cpu_avg_percent": cpu_avg,
            "ram_avg_mb": mem_avg,
            "workers": self.config.get("num_workers", ""),
            "cores_per_worker": self.config.get("cores_per_worker", ""),
            "ram_per_worker_gb": self.config.get("ram_per_worker_gb", ""),
            "notes": self.config.get("notes", "")
        }

        with open(self.log_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self._fieldnames())
            writer.writerow(row)

        print(
            f"🔹 [{row['framework']}] {operation} | {duration:.4f}s | "
            f"{cpu_avg:.2f}% CPU | {mem_avg:.2f}MB RAM | {records} records"
        )
