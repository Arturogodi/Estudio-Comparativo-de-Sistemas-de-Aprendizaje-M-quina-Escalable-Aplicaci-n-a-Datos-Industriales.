from ray.train.sklearn import SklearnTrainer
from ray.train import ScalingConfig
from sklearn.linear_model import LinearRegression
import ray
import pandas as pd
import time

class RayDistributedMLProcessor:
    def __init__(self, parquet_path):
        ray.init(ignore_reinit_error=True)
        self.ds = ray.data.read_parquet(parquet_path).select_columns(
            ["rating", "helpful_vote", "text", "verified_purchase"]
        )

    def prepare_data(self):
        def preprocess(batch: pd.DataFrame):
            batch["text_len"] = batch["text"].astype(str).str.len()
            batch["verified_purchase"] = batch["verified_purchase"].astype(int)
            return batch[["rating", "helpful_vote", "text_len", "verified_purchase"]].dropna()
        return self.ds.map_batches(preprocess, batch_format="pandas")

    def train_distributed(self):
        train_data = self.prepare_data()

        def train_fn(config):
            import pandas as pd
            from sklearn.linear_model import LinearRegression
            from sklearn.metrics import mean_squared_error
            from sklearn.model_selection import train_test_split

            df = config["data"]
            X = df[["helpful_vote", "text_len", "verified_purchase"]]
            y = df["rating"]
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

            model = LinearRegression()
            model.fit(X_train, y_train)
            preds = model.predict(X_test)
            rmse = mean_squared_error(y_test, preds, squared=False)
            print(f"Distributed RMSE: {rmse:.4f}")

        trainer = SklearnTrainer(
            train_loop_per_worker=train_fn,
            train_loop_config={"data": train_data.to_pandas()},
            scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
        )

        result = trainer.fit()
        return result
