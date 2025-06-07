import ray
import time

class RayProcessor:
    def __init__(self, parquet_path):
        ray.init(ignore_reinit_error=True)
        self.ds = ray.data.read_parquet(parquet_path)

    def filter_high_ratings(self):
        return self.ds.filter(lambda row: row["rating"] >= 4)

    def avg_helpful_by_year(self):
        # Crear columna "year" a partir del timestamp
        def add_year(row):
            import datetime
            row = row.copy()
            ts = int(row["timestamp"]) // 1000
            row["year"] = datetime.datetime.utcfromtimestamp(ts).year
            return row

        ds_with_year = self.ds.map(add_year)
        return ds_with_year.group_by("year").mean("helpful_vote")

    def count_verified(self):
        return self.ds.group_by("verified_purchase").count()

    def run_all(self):
        results = {}

        t0 = time.time()
        filtered = self.filter_high_ratings()
        filtered.materialize()
        results["filter_high_ratings"] = time.time() - t0

        t0 = time.time()
        avg_votes = self.avg_helpful_by_year()
        avg_votes.materialize()
        results["avg_helpful_by_year"] = time.time() - t0

        t0 = time.time()
        verified = self.count_verified()
        verified.materialize()
        results["count_verified"] = time.time() - t0

        return results
