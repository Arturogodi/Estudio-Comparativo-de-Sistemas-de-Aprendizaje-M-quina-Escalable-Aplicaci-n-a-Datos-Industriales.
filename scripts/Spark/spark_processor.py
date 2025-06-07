from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, from_unixtime, avg, count
import time

class SparkProcessor:
    def __init__(self, parquet_path):
        self.spark = SparkSession.builder \
            .appName("AmazonReviewsSpark") \
            .getOrCreate()
        self.df = self.spark.read.parquet(parquet_path)
    
    def filter_high_ratings(self):
        return self.df.filter(col("rating") >= 4)

    def avg_helpful_by_year(self):
        df = self.df.withColumn("year", year(from_unixtime(col("timestamp") / 1000)))
        return df.groupBy("year").agg(avg("helpful_vote").alias("avg_helpful"))

    def count_verified(self):
        return self.df.groupBy("verified_purchase").agg(count("*").alias("count"))

    def run_all(self):
        results = {}
        
        t0 = time.time()
        filtered = self.filter_high_ratings()
        filtered.count()
        results["filter_high_ratings"] = time.time() - t0

        t0 = time.time()
        avg_votes = self.avg_helpful_by_year()
        avg_votes.count()
        results["avg_helpful_by_year"] = time.time() - t0

        t0 = time.time()
        verified = self.count_verified()
        verified.count()
        results["count_verified"] = time.time() - t0

        return results
