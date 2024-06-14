from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
import logging

#Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Create a Spark session
def create_spark_session():
    try:
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("Unify Lookup District") \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")

#Function to read a collection from MongoDB
def read_collection(spark, vm_host, mongodb_port, db_name, collection_name):
    uri = f"mongodb://{vm_host}:{mongodb_port}/{db_name}.{collection_name}"
    df = spark.read.format("mongo") \
        .option('uri', uri) \
        .option('encoding', 'utf-8-sig') \
        .load()
    return df

#Function to write a DataFrame to a collection in MongoDB
def write_to_collection(vm_host, mongodb_port, db_name, collection_name, dataframe):
    uri = f"mongodb://{vm_host}:{mongodb_port}/{db_name}.{collection_name}"
    dataframe.write.format("mongo") \
        .option("uri", uri) \
        .option("encoding", "utf-8-sig") \
        .mode("overwrite") \
        .save()

#Function to drop duplicates and save new collection
def drop_duplicates_and_save_new_collection(spark, vm_host, mongodb_port, db_name, collections):
    try:
        #Iterate over the collections 
        for collection in collections:
            #First read the collection from MongoDB and store it in a DataFrame
            logger.info(f'Read collection "{collection}" from MongoDB.')
            df = read_collection(spark, vm_host, mongodb_port, db_name, collection)
            #Drop duplicates based on the columns "neigh_name", "district_id", and "district_name"
            logger.info(f'Dropping duplicates for collection "{collection}".')
            deduplicated_df = df.dropDuplicates(["neigh_name ", "district_id", "district_name"])
            
            #Sort the data by "_id"  because it loses the order when deduplicating
            logger.info(f'Sorting data for collection "{collection}".')
            sorted_df = deduplicated_df.orderBy("_id")
            
            #Write the deduplicated data to a new collection in MongoDB with the name "{collection}_deduplicated"
            new_collection_name = f"{collection}_deduplicated"
            logger.info(f'Writing deduplicated data to new collection "{new_collection_name}" in MongoDB.')
            write_to_collection(vm_host, mongodb_port, db_name, new_collection_name, sorted_df)
            logger.info(f'Deduplicated data for "{collection}" written to new collection "{new_collection_name}" in MongoDB.')
    except Exception as e:
        logger.error(f"Error processing collections for deduplication: {e}")
