from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
import time

class SparkMLProcessor:
    def __init__(self, parquet_path):
        self.spark = SparkSession.builder.appName("AmazonMLSpark").getOrCreate()
        df = self.spark.read.parquet(parquet_path)

        # Preprocesamiento
        df = df.select("rating", "helpful_vote", "text", "verified_purchase") \
               .withColumn("text_len", length(col("text")).cast("double")) \
               .withColumn("verified_purchase", col("verified_purchase").cast("double")) \
               .dropna()
        self.df = df

    def train_regression(self):
        assembler = VectorAssembler(
            inputCols=["helpful_vote", "text_len", "verified_purchase"],
            outputCol="features"
        )
        data = assembler.transform(self.df).select("features", "rating")

        # Split de entrenamiento y test
        train, test = data.randomSplit([0.8, 0.2], seed=42)

        # Entrenamiento
        lr = LinearRegression(labelCol="rating")
        start = time.time()
        model = lr.fit(train)
        elapsed_train = time.time() - start

        # Evaluación
        predictions = model.transform(test)
        evaluator = RegressionEvaluator(labelCol="rating", predictionCol="prediction", metricName="rmse")
        rmse = evaluator.evaluate(predictions)

        return {"rmse": rmse, "training_time": elapsed_train}
