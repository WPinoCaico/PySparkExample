from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc

# Paso 1: Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("ETL Test") \
    .getOrCreate()

# Paso 2: Diseño del esquema de datos
# Supongamos que tenemos una tabla "transactions" con columnas: customer_id, product_id, amount, timestamp, etc.
# Y una tabla "customers" con columnas: customer_id, name, address, etc.

# Paso 3: Extracción de datos
data = spark.read.csv("ruta/del/archivo.csv", header=True)

# Paso 4: Transformación de datos
# Cálculo del total de ventas por cliente
sales_per_customer = data.groupBy("customer_id").agg(sum("amount").alias("total_sales"))

# Productos más vendidos
top_products = data.groupBy("product_id").agg(sum("amount").alias("total_amount")) \
    .orderBy(desc("total_amount")).limit(10)

# Paso 5: Carga de datos
sales_per_customer.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydatabase") \
    .option("dbtable", "sales_per_customer") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .save()

top_products.write.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydatabase") \
    .option("dbtable", "top_products") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .save()

# Paso 6: Consultas y análisis
# Consulta 1: Clientes con mayor valor de compra
top_customers = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/mydatabase") \
    .option("dbtable", "sales_per_customer") \
    .option("user", "myuser") \
    .option("password", "mypassword") \
    .load()

top_customers.orderBy(desc("total_sales")).show()

# Consulta 2: Patrones de compra por región
# Supongamos que tenemos una tabla "orders" con columnas: customer_id, region, order_date, etc.
# Y una tabla "products" con columnas: product_id, name, category, etc.

purchase_patterns = data.join(orders, "customer_id").join(products, "product_id") \
    .groupBy("region", "category").agg(sum("amount").alias("total_amount"))

purchase_patterns.show()

# Paso 7: Documentacióncl
# Asegúrate de proporcionar una documentación ara y concisa de todo el proceso realizado, 
# incluyendo el diseño del esquema, el código y cualquier consideración relevante.

# Finalizar sesión de Spark
spark.stop()
