import logging
from data_formatters import *

#Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Main function
def main():
    #Different host and port for the MongoDB
    vm_host = 'localhost'
    mongodb_port = 27017
    persistent_db = 'PersistentZone'
    formatted_db = 'FormattedZone'
    #collections = ['Income_OpenBCN','Density_OpenCBN','Rent_Idealista']  #List of collections, maybe we can change this later


    #Estaria be fer que tot aixo fos amb inputs dient que vols fer y a quina coleccio vols, 
    
    #Create a Spark session
    spark = create_spark_session()

    #Drop duplicates and save new collection
    drop_duplicates_and_save_new_collection(spark, vm_host, mongodb_port, persistent_db, ['Income_OpenBCN','Density_OpenCBN','Rent_Idealista'])
    
    schema_income = StructType([
        StructField("_id", IntegerType(), nullable=False),
        StructField("neigh_name", StringType(), nullable=False),
        StructField("district_id", IntegerType(), nullable=False),
        StructField("district_name", StringType(), nullable=False),
        StructField("info", ArrayType(StructType([
            StructField("year", IntegerType(), nullable=True),
            StructField("pop", IntegerType(), nullable=True),
            StructField("RFD", DoubleType(), nullable=True)
        ])), nullable=True)])
    
    change_collection_schema(spark, vm_host, mongodb_port, persistent_db, formatted_db, "Income_OpenBCN", schema_income)


        # Merge lookup district tables
    merge_lookup_district_tables(spark, vm_host, mongodb_port, persistent_db, formatted_db,
                                 "lookup_table_district", "Income_Lookup_District", "Rent_Lookup_district","Rent_Lookup_district") #Change names if needed

    # Merge lookup neighborhood tables
    merge_lookup_neighborhood_tables(spark, vm_host, mongodb_port, persistent_db, formatted_db,
                                     "lookup_table_neighborhood", "Income_Lookup_Neighborhood", "Rent_Lookup_neighborhood","Density_Lookup_Neighborhood")  #Change names if needed

if __name__ == "__main__":
    main()
