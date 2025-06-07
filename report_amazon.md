# Informe Exploratorio — Dataset Amazon Reviews 2023

A continuación se presenta un resumen de los archivos `.parquet` procesados desde el directorio `data/input/amazon2023`.

| Archivo | Tamaño | Filas | Columnas | Columnas principales | Nulos totales |
|---------|--------|--------|----------|-----------------------|----------------|
| meta_Books.parquet | 7.3 GB | 4,448,181 | 16 | main_category, title, average_rating, rating_number, features... | 6,680,995 |
| meta_Clothing_Shoes_and_Jewelry.parquet | 5.5 GB | 7,218,481 | 16 | main_category, title, average_rating, rating_number, features... | 22,612,535 |
| meta_Electronics.parquet | 1.8 GB | 1,610,012 | 16 | main_category, title, average_rating, rating_number, features... | 4,944,563 |
| meta_Home_and_Kitchen.parquet | 4.1 GB | 3,735,584 | 16 | main_category, title, average_rating, rating_number, features... | 11,653,573 |
| meta_Toys_and_Games.parquet | 927.8 MB | 890,874 | 16 | main_category, title, average_rating, rating_number, features... | 2,699,798 |
| reviews_Books.parquet | 8.9 GB | 29,475,453 | 10 | rating, title, text, images, asin... | 0 |
| reviews_Clothing_Shoes_and_Jewelry.parquet | 9.6 GB | 66,033,346 | 10 | rating, title, text, images, asin... | 0 |
| reviews_Electronics.parquet | 8.9 GB | 43,886,944 | 10 | rating, title, text, images, asin... | 0 |
| reviews_Home_and_Kitchen.parquet | 11.4 GB | 67,409,944 | 10 | rating, title, text, images, asin... | 0 |
| reviews_Toys_and_Games.parquet | 2.7 GB | 16,260,406 | 10 | rating, title, text, images, asin... | 0 |
