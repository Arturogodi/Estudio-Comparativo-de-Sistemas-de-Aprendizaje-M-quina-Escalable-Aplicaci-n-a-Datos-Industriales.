import ray

# Conectarse al clúster Ray que está corriendo en Docker
ray.init(address="ray://localhost:10001")

@ray.remote
def f(x):
    return x * x

futures = [f.remote(i) for i in range(10)]
results = ray.get(futures)

print("Resultados:", results)
