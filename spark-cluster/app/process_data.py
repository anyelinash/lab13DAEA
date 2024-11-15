import time
from pyspark.sql import SparkSession

# Iniciar el cronómetro
start_time = time.time()

# Crear la sesión de Spark con configuración optimizada
spark = SparkSession.builder \
    .appName("MovieLens Optimization") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.instances", "4") \
    .getOrCreate()

# Leer el archivo CSV y aplicar coalesce para reducir particiones
data = spark.read.option("header", "true").csv("file:///data/movies.csv").coalesce(4)

# Cache de los datos para mejorar el rendimiento en operaciones subsecuentes
data.cache()

# Agrupar los géneros y contar la frecuencia de cada uno
result = data.groupBy("genres").count()

# Mostrar los resultados
result.show()

# Detener la sesión de Spark
spark.stop()

# Calcular el tiempo de ejecución
end_time = time.time()
execution_time = end_time - start_time

# Mostrar el tiempo de ejecución
print(f"Tiempo total de ejecución: {execution_time} segundos")
