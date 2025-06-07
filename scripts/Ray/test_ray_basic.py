import ray
import ray.data

# Iniciar Ray (usando el cluster ya existente si está activo)
ray.init(address="auto")

# Crear un dataset en memoria
data = [{"name": "Alice", "age": 25},
        {"name": "Bob", "age": 30},
        {"name": "Charlie", "age": 35}]
ds = ray.data.from_items(data)

# Mostrar contenido y conteo
ds.show()
print(f"🔢 Número de filas: {ds.count()}")

# Cerrar Ray (opcional en entorno distribuido)
ray.shutdown()
