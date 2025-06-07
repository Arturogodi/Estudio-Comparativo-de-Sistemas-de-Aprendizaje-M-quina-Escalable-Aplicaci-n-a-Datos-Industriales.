#!/bin/bash

# Nombre del archivo: build_and_launch_C1.sh

# Detener ejecución si algo falla
set -e

echo "🔧 Construyendo imagen Docker personalizada para Spark..."
docker build -t mi-spark-py39 -f Dockerfile.spark-3.5.5-py3.9.13 .

echo "🚀 Levantando entorno Docker (composición C1)..."
docker compose -f docker-compose_C1.yml up --build -d

echo "✅ Composición C1 desplegada con éxito."
