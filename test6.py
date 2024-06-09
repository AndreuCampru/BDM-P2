from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import os 
#os.environ['PYSPARK_PYTHON'] = 'C:\\Users\\pgome\\AppData\\Local\\Microsoft\\WindowsApps\\python3.12.exe'
#os.environ['PYSPARK_PYTHON'] = 'C:\\Program Files\\Python310\\python.exe'
python_path = 'C:\\Users\\pgome\\miniconda3\\envs\\bdm\\python.exe'
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_path
# Configuración de Spark
conf = SparkConf().setAppName("MongoDBIntegration") \
    .setMaster("local[*]") \
    .set("spark.mongodb.input.uri", "mongodb://localhost:27017/PersistentZone.OpenBCN") \
    .set("spark.mongodb.output.uri", "mongodb://localhost:27017/PersistentZone.OpenBCN") \
    .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")

# Crear SparkContext
sc = SparkContext(conf=conf)

# Configurar log4j
log4j = sc._jvm.org.apache.log4j
log4j.LogManager.getLogger("org.apache.spark.util.ShutdownHookManager").setLevel(log4j.Level.OFF)
log4j.LogManager.getLogger("org.apache.spark.SparkEnv").setLevel(log4j.Level.ERROR)

# Crear SparkSession
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.format("mongo").load()

# Si prefieres trabajar directamente con RDD en lugar de DataFrames:
rdd = df.rdd

# Mostrar algunos de los datos para ver qué contiene la colección
print(rdd.take(5))


# Ejemplo para filtrar documentos y seleccionar un campo específico
filtered_rdd = rdd.filter(lambda x: 'district_id' in x and x['district_id'] == "1")
mapped_rdd = filtered_rdd.map(lambda x: x['neigh_name'])

# Ver resultados
print(mapped_rdd.collect())

sc.stop()

