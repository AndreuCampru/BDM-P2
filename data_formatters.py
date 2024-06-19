from datetime import datetime
from pymongo import MongoClient
from pyspark.sql.functions import col, lower, regexp_replace, udf, lit, explode, array_union, array_distinct
from pyspark.sql.types import DoubleType

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, when, udf, lit, explode, collect_list, struct, collect_set
import logging
from pyspark.sql.types import *
import jellyfish # for similarity jaro_winkler_udf

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
        .option("replaceDocument", "true") \
        .mode("append") \
        .save()

#Function to drop duplicates and save new collection
def drop_duplicates_and_save_new_collection(spark, vm_host, mongodb_port, source_db_name, sink_db_name, collections):
    try:
        #Iterate over the collections 
        for collection in collections:
            #First read the collection from MongoDB and store it in a DataFrame
            logger.info(f'Read collection "{collection}" from MongoDB.')
            df = read_collection(spark, vm_host, mongodb_port, source_db_name, collection)
            #Drop duplicates based on the columns "neigh_name", "district_id", and "district_name"
            logger.info(f'Dropping duplicates for collection "{collection}".')
            
            if collection == 'Income_OpenBCN': #OpenBCN we choose the column "_id" to remove duplicates
                deduplicated_df = df.dropDuplicates(["_id"])
            elif collection == 'Rent_Idealista': #Idealista the date is the id, we will choose that to remove duplicates
                deduplicated_df = df.dropDuplicates(["_id"])
            elif collection == 'Density_OpenBCN':
                  deduplicated_df = df.dropDuplicates(["_id"])
                
            #If the collection is named "Income_lookup_district" or "Rebt_lookup_district", we will drop duplicates based on the column "district_id"
            if collection == 'Income_lookup_district' or collection == 'Income_lookup_neighborhood' or collection == 'Rent_lookup_district' or collection == 'Rent_lookup_neigh':
                deduplicated_df = df.dropDuplicates(["_id"])
            if collection == 'Density_lookup_district' or collection == 'Density_lookup_neighborhood':
                deduplicated_df = df.dropDuplicates(["_id"])            

            #Sort the data by "_id"  because it loses the order when deduplicating
            logger.info(f'Sorting data for collection "{collection}".')
            sorted_df = deduplicated_df.orderBy("_id")
            
            #Write the deduplicated data to a new collection in MongoDB with the name "{collection}_deduplicated"
            new_collection_name = f"{collection}_deduplicated"
            logger.info(f'Writing deduplicated data to new collection "{new_collection_name}" in MongoDB.')
            write_to_collection(vm_host, mongodb_port, sink_db_name, new_collection_name, sorted_df)
            logger.info(f'Deduplicated data for "{collection}" written to new collection "{new_collection_name}" in MongoDB.\n')
    except Exception as e:
        logger.error(f"Error processing collections for deduplication: {e}\n")




#Function to merge lookup distric tables into only one and send it to the formatted zone
def merge_lookup_district_tables(spark, vm_host, mongodb_port, persistent_db, formatted_db, output_collection, input_collection1, input_collection2, input_collection3):
    try:
        #Read the collections from MongoDB that we specified, we will input the lookup district tables
        logger.info(f"Reading '{input_collection1}' collection from MongoDB...")
        df1 = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection1)
        
        logger.info(f"Reading '{input_collection2}' collection from MongoDB...")
        df2 = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection2)
        
        logger.info(f"Reading '{input_collection3}' collection from MongoDB...")
        df3 = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection3)

        # Ensure all columns have the correct names before merging
        def rename_columns(df, col_mapping):
            for original_name, new_name in col_mapping.items():
                if original_name in df.columns:
                    df = df.withColumnRenamed(original_name, new_name)
            return df

        col_mapping = {
            "di": "district",
            "di_n": "district_name",
            "di_re": "district_reconciled",
            "ne_id": "neighborhood_id"
        }

        df1 = rename_columns(df1, col_mapping)
        df2 = rename_columns(df2, col_mapping)
        df3 = rename_columns(df3, col_mapping)            
        
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

        # Ensure all columns have the correct names before merging
        def rename_columns(df, col_mapping):
            for original_name, new_name in col_mapping.items():
                if original_name in df.columns:
                    df = df.withColumnRenamed(original_name, new_name)
            return df

        col_mapping = {
            "ne": "neighborhood",
            "ne_n": "neighborhood_name",
            "ne_re": "neighborhood_reconciled"
        }

        df1 = rename_columns(df1, col_mapping)
        df2 = rename_columns(df2, col_mapping)
        df3 = rename_columns(df3, col_mapping)
        
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
        new_df = df
        #Change the data types of the columns in the DataFrame following to the new schema
        for field in schema.fields:
            if field.name in df.columns:
                new_df = df.withColumn(field.name, col(field.name).cast(field.dataType))

        #Write the DataFrame with the new schema to a new collection in MongoDB
        write_to_collection(host, port, target_db, collection, new_df)
        logger.info(f"Successfully converted data types for collection '{collection}'.")
    except Exception as e:
        logger.error(f"Error while converting data types for collection '{collection}': {e}")

def rename_collection_columns(spark, host, port, db, collection, rename_mapping):
    try:
        # Read the collection from MongoDB
        df = read_collection(spark, host, port, db, collection)
        if df is None:
            logger.error(f"No data found in collection '{collection}' in database '{db}'")
            return

        # Rename columns based on the provided mapping
        for old_name, new_name in rename_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
            elif f"info.{old_name}" in df.columns:
                df = df.withColumn(f"info.{new_name}", col(f"info.{old_name}")).drop(f"info.{old_name}")
            else:
                logger.warning(f"Column '{old_name}' not found in collection '{collection}'")


        # Write the DataFrame with renamed columns to the same collection in MongoDB
        write_to_collection(host, port, db, collection, df)
        logger.info(f"Successfully renamed columns for collection '{collection}'.")
    except Exception as e:
        logger.error(f"Error while renaming columns for collection '{collection}': {e}")

        
        
# def reconcile_data(spark, vm_host, mongodb_port, persistent_db, formatted_db, input_collection, lookup_district_collection, lookup_neighborhood_collection, output_collection):
#     try:
#         # Read input collection
#         input_df = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection)

#         # Read lookup collections
#         lookup_district_df = read_collection(spark, vm_host, mongodb_port, formatted_db, lookup_district_collection)
#         lookup_neighborhood_df = read_collection(spark, vm_host, mongodb_port, formatted_db, lookup_neighborhood_collection)

#         # Preprocess join columns to lower case and remove accents
#         input_df = input_df.withColumn("district_name", lower(regexp_replace(col("district_name"), "[\u0300-\u036F]", "")))
#         input_df = input_df.withColumn("neighborhood_name", lower(regexp_replace(col("neighborhood_name"), "[\u0300-\u036F]", "")))
        
#         lookup_district_df = lookup_district_df.withColumn("district_name", lower(regexp_replace(col("district_name"), "[\u0300-\u036F]", "")))
#         lookup_neighborhood_df = lookup_neighborhood_df.withColumn("neighborhood_name", lower(regexp_replace(col("neighborhood_name"), "[\u0300-\u036F]", "")))

#         # Reconcile district
#         reconciled_df = input_df.join(lookup_district_df, input_df["district_name"] == lookup_district_df["district_name"], "left") \
#                                 .select(input_df["*"], lookup_district_df["district_reconciled"]) \
#                                 .drop(lookup_district_df["district_name"])

#         # Reconcile neighborhood
#         reconciled_df = reconciled_df.join(lookup_neighborhood_df, reconciled_df["neighborhood_name"] == lookup_neighborhood_df["neighborhood_name"], "left") \
#                                      .select(reconciled_df["*"], lookup_neighborhood_df["neighborhood_reconciled"].alias("neigh_name_reconciled")) \
#                                      .drop(lookup_neighborhood_df["neighborhood_name"])

#         reconciled_df = reconciled_df.dropDuplicates(["_id"])

#         # Write the reconciled data to a new collection in MongoDB
#         write_to_collection(vm_host, mongodb_port, formatted_db, output_collection, reconciled_df)
#         logger.info(f"Reconciled data written to collection '{output_collection}' in MongoDB.")
#     except Exception as e:
#         logger.error(f"Error reconciling data: {e}")

from pyspark.sql.functions import col, lower, regexp_replace, explode

def reconcile_data(spark, vm_host, mongodb_port, persistent_db, formatted_db, input_collection, lookup_district_collection, lookup_neighborhood_collection, output_collection):
    try:
        # Read input collection
        input_df = read_collection(spark, vm_host, mongodb_port, persistent_db, input_collection)

        # Check if the collection has a 'value' field and it's an array
        if 'value' in input_df.columns:
            # Explode the 'value' field to access nested fields
            exploded_df = input_df.withColumn("value", explode(col("value"))).select(col("_id"), col("value.*"))
            # Rename the 'neighborhood' column to 'neighborhood_name' to match the other collections
            exploded_df = exploded_df.withColumnRenamed("neighborhood", "neighborhood_name")
        else:
            exploded_df = input_df

        # Read lookup collections
        lookup_district_df = read_collection(spark, vm_host, mongodb_port, formatted_db, lookup_district_collection)
        lookup_neighborhood_df = read_collection(spark, vm_host, mongodb_port, formatted_db, lookup_neighborhood_collection)

        # Preprocess join columns to lower case and remove accents
        exploded_df = exploded_df.withColumn("district_name", lower(regexp_replace(col("district_name"), "[\u0300-\u036F]", "")))
        exploded_df = exploded_df.withColumn("neighborhood_name", lower(regexp_replace(col("neighborhood_name"), "[\u0300-\u036F]", "")))
        
        lookup_district_df = lookup_district_df.withColumn("district_name", lower(regexp_replace(col("district_name"), "[\u0300-\u036F]", "")))
        lookup_neighborhood_df = lookup_neighborhood_df.withColumn("neighborhood_name", lower(regexp_replace(col("neighborhood_name"), "[\u0300-\u036F]", "")))

        # Reconcile district
        reconciled_df = exploded_df.join(lookup_district_df, exploded_df["district_name"] == lookup_district_df["district_name"], "left") \
                                .select(exploded_df["*"], lookup_district_df["district_reconciled"]) \
                                .drop(lookup_district_df["district_name"])

        # Reconcile neighborhood
        reconciled_df = reconciled_df.join(lookup_neighborhood_df, reconciled_df["neighborhood_name"] == lookup_neighborhood_df["neighborhood_name"], "left") \
                                     .select(reconciled_df["*"], lookup_neighborhood_df["neighborhood_reconciled"].alias("neigh_name_reconciled")) \
                                     .drop(lookup_neighborhood_df["neighborhood_name"])

        reconciled_df = reconciled_df.dropDuplicates(["_id"])

        # Write the reconciled data to a new collection in MongoDB
        write_to_collection(vm_host, mongodb_port, formatted_db, output_collection, reconciled_df)
        logger.info(f"Reconciled data written to collection '{output_collection}' in MongoDB.")
    except Exception as e:
        logger.error(f"Error reconciling data: {e}")