from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
import logging
from pyspark.sql.types import *

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
            
            if collection == 'Income_OpenBCN': #OpenBCN we choose the columns "neigh_name", "district_id", and "district_name" to remove duplicates
                deduplicated_df = df.dropDuplicates(["neigh_name ", "district_id", "district_name"])
            elif collection == 'Rent_Idealista': #Idealista the date is the id, we will choose that to remove duplicates
                deduplicated_df = df.dropDuplicates(["_id"])
                
            #If the collection is named "Income_lookup_district" or "Rebt_lookup_district", we will drop duplicates based on the column "district_id"
            if collection == 'Income_lookup_district' or collection == 'Income_lookup_neighborhood' or collection == 'Rebt_lookup_district' or collection == 'Rent_lookup_neigh':
                deduplicated_df = df.dropDuplicates(["_id"])

            #Sort the data by "_id"  because it loses the order when deduplicating
            logger.info(f'Sorting data for collection "{collection}".')
            sorted_df = deduplicated_df.orderBy("_id")
            
            #Write the deduplicated data to a new collection in MongoDB with the name "{collection}_deduplicated"
            new_collection_name = f"{collection}_deduplicated"
            logger.info(f'Writing deduplicated data to new collection "{new_collection_name}" in MongoDB.')
            write_to_collection(vm_host, mongodb_port, db_name, new_collection_name, sorted_df)
            logger.info(f'Deduplicated data for "{collection}" written to new collection "{new_collection_name}" in MongoDB.\n')
    except Exception as e:
        logger.error(f"Error processing collections for deduplication: {e}\n")




#Function to merge lookup distric tables into only one and send it to the formatted zone
def merge_lookup_district_tables(spark, vm_host, mongodb_port, persistent_db, formatted_db, output_collection, input_collection1, input_collection2,input_collection3):
    try:
        #Read the collections from MongoDB that we specified, we will input the lookup district tables
        logger.info(f"Reading '{input_collection1}' collection from MongoDB...")
        df1 = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection1)
        
        logger.info(f"Reading '{input_collection2}' collection from MongoDB...")
        df2 = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection2)
        
        logger.info(f"Reading '{input_collection3}' collection from MongoDB...")
        df3 = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection3)
        
        #Make the union of the tables and drop duplicates based on the column "_id"
        logger.info("Merging lookup district tables...")
        merged_df = df1.union(df2).union(df3).dropDuplicates(["_id"])
        
        #Write the merged data to a new collection in MongoDB with the name "lookup_table_district" in the formatted zone
        logger.info(f"Writing merged data to collection '{output_collection}' in MongoDB.")
        write_to_collection(vm_host, mongodb_port, formatted_db, output_collection, merged_df)
        logger.info(f"Merged lookup district tables written to collection '{output_collection}' in MongoDB.\n")
    except Exception as e:
        logger.error(f"Error merging lookup district tables: {e}\n")
        
        
        
#Same as the previous function but with the neighborhood tables
#Function to merge lookup neighborhood tables into only one and send it to the formatted zone
def merge_lookup_neighborhood_tables(spark, vm_host, mongodb_port, persistent_db, formatted_db, output_collection, input_collection1, input_collection2, input_collection3):
    try:
        #Read the collections from MongoDB that we specified, we will input the lookup neighborhood tables
        logger.info(f"Reading '{input_collection1}' collection from MongoDB...")
        df1 = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection1)
        
        logger.info(f"Reading '{input_collection2}' collection from MongoDB...")
        df2 = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection2)
        
        logger.info(f"Reading '{input_collection3}' collection from MongoDB...")
        df3 = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection3)
        
        #Make the union of the tables and drop duplicates based on the column "_id"
        logger.info("Merging lookup neighborhood tables...")
        merged_df = df1.union(df2).union(df3).dropDuplicates(["_id"])
        
        #Write the merged data to a new collection in MongoDB with the name "lookup_table_neighborhood" in the formatted zone
        logger.info(f"Writing merged data to collection '{output_collection}' in MongoDB.")
        write_to_collection(vm_host, mongodb_port, formatted_db, output_collection, merged_df)
        logger.info(f"Merged lookup neighborhood tables written to collection '{output_collection}' in MongoDB.\n")
    except Exception as e:
        logger.error(f"Error merging lookup neighborhood tables: {e}\n")
        
        
        
#Create new schema for a collection, changing the data types of the columns
def change_collection_schema(spark, host, port, source_db, target_db, collection, schema):
    try:
        #Read the collection from MongoDB
        df = read_collection(spark, host, port, source_db, collection)

        #Change the data types of the columns in the DataFrame following to the new schema
        for field in schema.fields:
            if field.name in df.columns:
                new_df = df.withColumn(field.name, col(field.name).cast(field.dataType))

        #Write the DataFrame with the new schema to a new collection in MongoDB
        write_to_collection(host, port, target_db, collection,new_df)
        logger.info(f"Successfully converted data types for collection '{collection}'.")
    except Exception as e:
        logger.error(f"Error while converting data types for collection '{collection}': {e}")



def reconcile_data_with_lookup(spark, vm_host, mongodb_port, persistent_db, formatted_db, input_collection, lookup_collection, output_collection, input_join_attribute, lookup_join_attribute, lookup_id, input_id_reconcile, threshold):
    try:
        logger.info(f"Reading '{input_collection}' collection from MongoDB...")
        input_df = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection)
        
        logger.info(f"Reading '{lookup_collection}' collection from MongoDB...")
        lookup_df = read_collection(spark, vm_host, mongodb_port, formatted_db, lookup_collection)
        
        input_df = input_df.withColumn(input_join_attribute, lower(regexp_replace(col(input_join_attribute), "[\u0300-\u036F]", "")))
        lookup_df = lookup_df.withColumn(lookup_join_attribute, lower(regexp_replace(col(lookup_join_attribute), "[\u0300-\u036F]", "")))
        
        logger.info("Performing join and reconciliation...")
        result_df = input_df.join(lookup_df,
                                  when(levenshtein(col(input_join_attribute), col(lookup_join_attribute)) <= threshold, True).otherwise(False),
                                  "left"
                                  ).withColumn(input_id_reconcile, col(lookup_id))
        
        result_df = result_df.dropDuplicates(["_id"])
        
        logger.info(f"Writing reconciled data to collection '{output_collection}' in MongoDB.")
        write_to_collection(vm_host, mongodb_port, formatted_db, output_collection, result_df)
        logger.info(f"Reconciled data written to collection '{output_collection}' in MongoDB.")
    except Exception as e:
        logger.error(f"Error reconciling data: {e}")