import time
import csv
from datetime import datetime

class Timer:
    def __init__(self, name="Unnamed"):
        self.name = name
        self.start_time = None
        self.end_time = None

    def start(self):
        self.start_time = time.time()
        print(f"[{self.name}] Timer started.")

    def stop(self):
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        print(f"[{self.name}] Timer stopped. Duration: {duration:.4f} seconds.")
        return duration

class BenchmarkLogger:
    def __init__(self, filename="benchmark_results.csv"):
        self.filename = filename

    def log(self, operation, duration):
        row = {
            "timestamp": datetime.now().isoformat(),
            "operation": operation,
            "duration_seconds": duration
        }
        file_exists = False
        try:
            with open(self.filename, 'r'):
                file_exists = True
        except FileNotFoundError:
            pass

        with open(self.filename, mode='a', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=row.keys())
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
        print(f"[LOG] Operation '{operation}' logged with duration {duration:.4f} seconds.")
