import ray
import time
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

class RayMLProcessor:
    def __init__(self, parquet_path):
        ray.init(ignore_reinit_error=True)
        ds = ray.data.read_parquet(parquet_path)

        # Seleccionar columnas necesarias
        self.ds = ds.select_columns(["rating", "helpful_vote", "text", "verified_purchase"])

    def prepare_dataframe(self):
        def preprocess(batch: pd.DataFrame):
            batch = batch.copy()
            batch["text_len"] = batch["text"].astype(str).str.len()
            batch["verified_purchase"] = batch["verified_purchase"].astype(int)
            return batch[["rating", "helpful_vote", "text_len", "verified_purchase"]]

        df = self.ds.map_batches(preprocess, batch_format="pandas").to_pandas()
        df = df.dropna()
        return df

    def train_regression(self):
        df = self.prepare_dataframe()

        X = df[["helpful_vote", "text_len", "verified_purchase"]]
        y = df["rating"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = LinearRegression()
        start = time.time()
        model.fit(X_train, y_train)
        elapsed_train = time.time() - start

        preds = model.predict(X_test)
        rmse = mean_squared_error(y_test, preds, squared=False)

        return {"rmse": rmse, "training_time": elapsed_train}
