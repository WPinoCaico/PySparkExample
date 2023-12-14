from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, count, avg

# Paso 1: Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("Web Logs Analysis") \
    .getOrCreate()

# Paso 2: Extracción de datos
logs = spark.read.text("ruta/del/archivo.txt")

# Paso 3: Limpieza de datos
logs_cleaned = logs.filter(logs.value != "")  # Eliminar registros vacíos

# Paso 4: Análisis de datos
# a. Las 10 URL más visitadas
top_urls = logs_cleaned.groupBy("value").agg(count("*").alias("count")) \
    .orderBy(desc("count")).limit(10)

# b. Código de respuesta más frecuente
most_frequent_response = logs_cleaned.groupBy("response_code").agg(count("*").alias("count")) \
    .orderBy(desc("count")).limit(1)

# c. Cliente con mayor cantidad de solicitudes
top_client = logs_cleaned.groupBy("client_ip").agg(count("*").alias("count")) \
    .orderBy(desc("count")).limit(1)

# d. Solicitudes por día
requests_per_day = logs_cleaned.groupBy(logs_cleaned.value.substr(1, 10).alias("day")).agg(count("*").alias("count"))

# e. Duración promedio de las solicitudes
average_duration = logs_cleaned.select(avg("duration").alias("average_duration"))

# Paso 5: Almacenamiento de resultados
top_urls.write.csv("ruta/del/archivo_top_urls.csv", header=True)
most_frequent_response.write.csv("ruta/del/archivo_most_frequent_response.csv", header=True)
top_client.write.csv("ruta/del/archivo_top_client.csv", header=True)
requests_per_day.write.csv("ruta/del/archivo_requests_per_day.csv", header=True)
average_duration.write.csv("ruta/del/archivo_average_duration.csv", header=True)

# Paso 6: Documentación
# Asegúrate de proporcionar una documentación clara y concisa de tu enfoque, incluyendo el diseño del código, las transformaciones realizadas y los resultados obtenidos.

# Finalizar sesión de Spark
spark.stop()