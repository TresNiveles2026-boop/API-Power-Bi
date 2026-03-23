# Usamos una versión oficial y ligera de Python
FROM python:3.11-slim

# Evita que Python escriba archivos .pyc y fuerza a que la salida de consola sea directa
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos primero el archivo de requerimientos (para aprovechar la caché de Docker)
COPY requirements.txt .

# Instalamos las librerías necesarias
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el resto de tu código a la carpeta /app
COPY . .

# Cloud Run de Google usa por defecto el puerto 8080
EXPOSE 8080

# Comando para ejecutar la aplicación cuando el contenedor se inicie
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
