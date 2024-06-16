# Description: This script is the main entry point for the data pipeline that processes the data and prepares it for analysis. The script consists of the following steps:
# 1) Data formatters
# 2) Descriptive analysis
# 3) Predictive analysis


# Import required libraries
import logging
from pyspark.sql.types import *

from data_formatters import (
    create_spark_session, 
    drop_duplicates_and_save_new_collection, 
    merge_lookup_district_tables, 
    merge_lookup_neighborhood_tables, 
    change_collection_schema, 
    reconcile_data
)


def main():

    ## Data formatters
    # Parameters
    vm_host = "localhost"
    mongodb_port = "27017"
    persistent_db = "persistent_landing_zone"
    formatted_db = "formatted_zone"
    
    # Collection names for deduplication
    collections = [
        "Income_OpenBCN", "Rent_Idealista", "Density_OpenBCN",
        "Income_lookup_district", "Income_lookup_neighborhood",
        "Rent_lookup_district", "Rent_lookup_neigh",
        "Density_lookup_district", "Density_lookup_neighborhood"
    ]
    
    # Collection names for merging
    district_collections = ["Income_lookup_district", "Rent_lookup_district", "Density_lookup_district"]
    neighborhood_collections = ["Income_lookup_neighborhood", "Rent_lookup_neigh", "Density_lookup_neighborhood"]
    
    # Collection names for schema change
    collections = [
        "Income_OpenBCN", "Rent_Idealista", "Density_OpenBCN",
        "lookup_table_district", "lookup_table_neighborhood"
        ]
    new_schema_lookup_neighborhood = StructType([
                    StructField("_id", StringType(), nullable=False),
                    StructField("neighborhood", StringType(), nullable=False),
                    StructField("neighborhood_name", StringType(), nullable=False),
                    StructField("neighborhood_reconciled", StringType(), nullable=False),
                    StructField("ne", StringType(), nullable=True),
                    StructField("ne_n", StringType(), nullable=True),
                    StructField("ne_re", StringType(), nullable=True),
                ])
    
    new_schema_lookup_district = StructType([
                    StructField("_id", StringType(), nullable=False),
                    StructField("district", StringType(), nullable=False),
                    StructField("district_name", StringType(), nullable=False),
                    StructField("district_reconciled", StringType(), nullable=False),
                    StructField("neighborhood_id", ArrayType(StringType()), nullable=False)
                ])  

    new_schema_idealista = StructType([
                    StructField("_id", StringType(), nullable=False),
                    StructField("value", ArrayType(StructType([
                        StructField("address", StringType(), nullable=True),
                        StructField("bathrooms", IntegerType(), nullable=True),
                        StructField("country", StringType(), nullable=True),
                        StructField("detailedType", StructType([
                            StructField("subTypology", StringType(), nullable=True),
                            StructField("typology", StringType(), nullable=True)
                        ]), nullable=True),
                        StructField("distance", StringType(), nullable=True),
                        StructField("district", StringType(), nullable=True),
                        StructField("exterior", BooleanType(), nullable=True),
                        StructField("externalReference", StringType(), nullable=True),
                        StructField("floor", IntegerType(), nullable=True),
                        StructField("has360", BooleanType(), nullable=True),
                        StructField("has3DTour", BooleanType(), nullable=True),
                        StructField("hasLift", BooleanType(), nullable=True),
                        StructField("hasPlan", BooleanType(), nullable=True),
                        StructField("hasStaging", BooleanType(), nullable=True),
                        StructField("hasVideo", BooleanType(), nullable=True),
                        StructField("latitude", DoubleType(), nullable=True),
                        StructField("longitude", DoubleType(), nullable=True),
                        StructField("municipality", StringType(), nullable=True),
                        StructField("neighborhood", StringType(), nullable=True),
                        StructField("newDevelopment", BooleanType(), nullable=True),
                        StructField("newDevelopmentFinished", BooleanType(), nullable=True),
                        StructField("numPhotos", IntegerType(), nullable=True),
                        StructField("operation", StringType(), nullable=True),
                        StructField("parkingSpace", StructType([
                            StructField("hasParkingSpace", BooleanType(), nullable=True),
                            StructField("isParkingSpaceIncludedInPrice", BooleanType(), nullable=True),
                            StructField("parkingSpacePrice", DoubleType(), nullable=True)
                        ]), nullable=True),
                        StructField("price", DoubleType(), nullable=True),
                        StructField("priceByArea", DoubleType(), nullable=True),
                        StructField("propertyCode", StringType(), nullable=True),
                        StructField("propertyType", StringType(), nullable=True),
                        StructField("province", StringType(), nullable=True),
                        StructField("rooms", IntegerType(), nullable=True),
                        StructField("showAddress", BooleanType(), nullable=True),
                        StructField("size", DoubleType(), nullable=True),
                        StructField("status", StringType(), nullable=True),
                        StructField("suggestedTexts", StructType([
                            StructField("subtitle", StringType(), nullable=True),
                            StructField("title", StringType(), nullable=True)
                        ]), nullable=True),
                        StructField("thumbnail", StringType(), nullable=True),
                        StructField("topNewDevelopment", BooleanType(), nullable=True),
                        StructField("url", StringType(), nullable=True)
                    ])))
                ])      

    new_schema_income = StructType([
                    StructField("_id", IntegerType(), nullable=False),
                    StructField("neigh_name", StringType(), nullable=False),
                    StructField("district_id", IntegerType(), nullable=False),
                    StructField("district_name", StringType(), nullable=False),
                    StructField("info", ArrayType(StructType([
                        StructField("year", IntegerType(), nullable=True),
                        StructField("pop", IntegerType(), nullable=True),
                        StructField("RFD", DoubleType(), nullable=True)
                    ])), nullable=True)
                ])  

    new_schema_density = StructType([
                    StructField("_id", StringType(), nullable=False),
                    StructField("neigh_name", StringType(), nullable=False),
                    StructField("district_id", StringType(), nullable=False),
                    StructField("district_name", StringType(), nullable=False),
                    StructField("info", ArrayType(StructType([
                        StructField("year", IntegerType(), nullable=True),
                        StructField("population", DoubleType(), nullable=True),
                        StructField("density (inh/ha)", DoubleType(), nullable=True),
                        StructField("net_density (inh/ha)", DoubleType(), nullable=True)
                    ])), nullable=True)
                ])
         
    # Collection names for reconciliation
    input_collections = ["Rent_Idealista_deduplicated", "Income_OpenBCN_deduplicated", "Density_OpenBCN_deduplicated"]
    lookup_district_collection = "lookup_table_district_deduplicated"
    lookup_neighborhood_collection = "lookup_table_neighborhood_deduplicted"
    output_collections = ["Rent_Idealista_reconciled", "Income_OpenBCN_reconciled", "Density_OpenBCN_reconciled"]
    
    #Configure the logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Create Spark session
    spark = create_spark_session()
    if spark is None:
        logger.error("Failed to create Spark session. Exiting...")
        return
    
    # Drop duplicates and save new collections
    drop_duplicates_and_save_new_collection(spark, vm_host, mongodb_port, persistent_db, formatted_db, collections)
    
    # Merge lookup district tables
    merge_lookup_district_tables(
        spark, vm_host, mongodb_port, 
        persistent_db, formatted_db, 
        "lookup_table_district", *district_collections
    )
    
    # Merge lookup neighborhood tables
    merge_lookup_neighborhood_tables(
        spark, vm_host, mongodb_port, 
        persistent_db, formatted_db, 
        "lookup_table_neighborhood", *neighborhood_collections
    )
    

    # Change collection schemas
    change_collection_schema(spark, vm_host, mongodb_port, persistent_db, formatted_db, "lookup_table_neighborhood", new_schema_lookup_neighborhood)
    change_collection_schema(spark, vm_host, mongodb_port, persistent_db, formatted_db, "lookup_table_district", new_schema_lookup_district)
    change_collection_schema(spark, vm_host, mongodb_port, persistent_db, formatted_db, "Rent_Idealista", new_schema_idealista)
    change_collection_schema(spark, vm_host, mongodb_port, persistent_db, formatted_db, "Income_OpenBCN", new_schema_income)
    change_collection_schema(spark, vm_host, mongodb_port, persistent_db, formatted_db, "Density_OpenBCN", new_schema_density)
    
    
    # Reconcile data for each input collection
    for input_collection, output_collection in zip(input_collections, output_collections):
        reconcile_data(
            spark, vm_host, mongodb_port, 
            persistent_db, formatted_db, 
            input_collection, lookup_district_collection, lookup_neighborhood_collection, output_collection
        )

    logger.info("Data processing pipeline completed successfully.")

if __name__ == "__main__":
    main()