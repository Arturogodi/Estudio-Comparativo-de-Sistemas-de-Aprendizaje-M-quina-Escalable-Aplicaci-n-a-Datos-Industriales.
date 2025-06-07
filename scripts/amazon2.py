import os

folder = "data/input/amazon"

for fname in os.listdir(folder):
    path = os.path.join(folder, fname)
    size_mb = os.path.getsize(path) / (1024 * 1024)
    with open(path, 'r', encoding='utf-8') as f:
        try:
            lines = sum(1 for _ in f)
        except UnicodeDecodeError:
            lines = "Error al leer archivo"
    print(f"{fname} — {size_mb:.2f} MB — {lines} líneas")
