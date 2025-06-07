import pandas as pd
import os

data_dir = "data/bronze/amazon2023"
files = os.listdir(data_dir)

meta_files = [f for f in files if f.startswith("meta_") and f.endswith(".parquet")]
review_files = [f for f in files if f.startswith("reviews_") and f.endswith(".parquet")]

summary = []

for meta_file in meta_files:
    category = meta_file.replace("meta_", "").replace(".parquet", "")
    review_file = f"reviews_{category}.parquet"

    try:
        meta_df = pd.read_parquet(os.path.join(data_dir, meta_file))
        n_products = meta_df["asin"].nunique()
    except Exception as e:
        print(f"Error en {meta_file}: {e}")
        n_products = 0

    if review_file in review_files:
        try:
            review_df = pd.read_parquet(os.path.join(data_dir, review_file))
            n_reviews = len(review_df)
            n_users = review_df["reviewerID"].nunique()
        except Exception as e:
            print(f"Error en {review_file}: {e}")
            n_reviews = 0
            n_users = 0
    else:
        n_reviews = 0
        n_users = 0

    summary.append({
        "category": category,
        "n_products": n_products,
        "n_reviews": n_reviews,
        "n_users": n_users
    })

summary_df = pd.DataFrame(summary).sort_values("n_reviews", ascending=False)
print(summary_df)
