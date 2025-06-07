### ✅ Tabla de configuraciones de ejecución

| Config | # Workers | Cores/Worker | RAM/Worker | Total Cores | Total RAM | Uso previsto                                               |
|--------|-----------|--------------|------------|-------------|-----------|-------------------------------------------------------------|
| **C1** | 2         | 2            | 8 GB       | 4           | 16 GB     | Baseline equilibrado: tareas ligeras, prueba rápida         |
| **C2** | 4         | 2            | 8 GB       | 8           | 32 GB     | Configuración recomendada: buen balance general             |
| **C3** | 4         | 2            | 4 GB       | 8           | 16 GB     | Comparativa por baja RAM: cuello de memoria                 |
| **C4** | 2         | 2            | 16 GB      | 4           | 32 GB     | Comparativa por RAM holgada: evitar spills, joins pesados   |

## 🔧 Requisitos para usar Ray en Docker

- **Imagen usada:** rayproject/ray:2.44.1
- **Python dentro del contenedor:** 3.9.21
- **Python en cliente (Windows):** 3.9.13
- **Ray en cliente:** 2.44.1 con el extra `[client]` → `pip install "ray[client]==2.44.1"`

**Nota:** Ray Client requiere que la versión de Ray y la versión de Python sean iguales (o muy próximas) entre cliente y servidor.

## 🐳 Cómo mantenemos el contenedor Ray activo

Por defecto, el contenedor de Ray (`ray-head` y `ray-worker`) se apagaba porque:
- El proceso principal (`ray start ...`) finalizaba si no había tareas activas.
- Además, el prompt de estadísticas bloqueaba el arranque.

**Soluciones aplicadas:**
1. Añadimos `--disable-usage-stats` en el comando de arranque para evitar el prompt.
2. Usamos `bash -c "comando && tail -f /dev/null"` en el `docker-compose.yml` para mantener el proceso activo.

Ejemplo de comando usado en el `docker-compose.yml`:

```yaml
command: >
  bash -c "ray start --head --port=6379 --dashboard-host 0.0.0.0 --dashboard-port=8265 --ray-client-server-port=10001 --disable-usage-stats && tail -f /dev/null"

## ❓ ¿Por qué crear una imagen personalizada de Spark con Python 3.9.13?

Las imágenes oficiales recientes de Spark (como `bitnami/spark`) utilizan versiones de Python superiores, como **3.12** o **3.13**.

Esto provocaba dos problemas principales:

1. **Incompatibilidad con el entorno local:**  
   PySpark no permite diferencias en la versión menor de Python entre el *driver* (mi equipo con Python 3.9.13) y los *workers* (contenedores que venían con Python 3.12/3.13). Esto causaba errores del tipo:

   `PYTHON_VERSION_MISMATCH: Python in worker has different version (3, 12) than that in driver 3.9.`

2. **Comparativa con Ray:**  
   Para realizar una comparativa justa (benchmark) entre Ray y Spark, necesitaba que ambos entornos usaran la **misma versión de Python (3.9.13)**. Ya que Ray utiliza Python 3.9.21 en el contenedor y el cliente usa 3.9.13, estandarizar todo a Python 3.9.x era la opción más lógica.

### 🔨 Solución aplicada

Se creó una imagen personalizada basada en Python 3.9.13, instalando sobre ella Spark 3.5.5 y PySpark 3.5.5. De esta forma, se eliminaban los errores de incompatibilidad y se podía garantizar que tanto Ray como Spark ejecutaban bajo las mismas versiones de Python en cliente y servidor.

from pathlib import Path

