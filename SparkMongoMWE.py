#
#First import data using
#mongoimport --jsonArray restaurants.json
#

from pyspark.sql import SparkSession

if __name__ == "__main__":
	spark = SparkSession \
		.builder \
		.master(f"local[*]") \
		.appName("myApp") \
		.config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
		.getOrCreate()

	restaurantsRDD = spark.read.format("mongo")\
		.option('uri', f"mongodb://127.0.0.1/test.restaurants") \
		.load() \
		.rdd

	restaurantsRDD.foreach(lambda r: print(r))