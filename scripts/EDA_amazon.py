import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def load_data(path):
    print(f"📂 Cargando datos desde {path}...")
    df = pd.read_parquet(path)
    return df

def basic_info(df):
    print("📌 Shape:", df.shape)
    print("🧾 Columnas:", df.columns.tolist())
    print("🧪 Muestra aleatoria:")
    print(df.sample(5))
    print("\n🧼 Info:")
    print(df.info())
    print("\n❓ Nulos:")
    print(df.isnull().sum())

def plot_rating_distribution(df, outdir):
    plt.figure()
    df['rating'].value_counts().sort_index().plot(kind='bar')
    plt.title('Distribución de ratings')
    plt.xlabel('Rating')
    plt.ylabel('Frecuencia')
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "rating_distribution.png"))
    plt.close()

def plot_reviews_by_year(df, outdir):
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df['year'] = df['timestamp'].dt.year
    plt.figure()
    df['year'].value_counts().sort_index().plot()
    plt.title('Número de reviews por año')
    plt.xlabel('Año')
    plt.ylabel('Cantidad de reviews')
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "reviews_by_year.png"))
    plt.close()

def plot_review_length(df, outdir):
    df['review_length'] = df['text'].astype(str).str.len()
    plt.figure()
    sns.histplot(df['review_length'], bins=50, kde=True)
    plt.title('Longitud de las reseñas')
    plt.xlabel('Número de caracteres')
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "review_length.png"))
    plt.close()

def main():
    parquet_path = "data/input/amazon2023/reviews_Books.parquet"
    output_dir = "outputs"
    os.makedirs(output_dir, exist_ok=True)

    df = load_data(parquet_path)
    basic_info(df)
    plot_rating_distribution(df, output_dir)
    plot_reviews_by_year(df, output_dir)
    plot_review_length(df, output_dir)

    print(f"✅ EDA completado. Gráficos guardados en: {output_dir}")

if __name__ == "__main__":
    main()
