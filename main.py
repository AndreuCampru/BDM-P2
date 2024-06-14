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
    collections = ['OpenBCN']  #List of collections, maybe we can change this later

    #Create a Spark session
    spark = create_spark_session()

    #Drop duplicates and save new collection
    drop_duplicates_and_save_new_collection(spark, vm_host, mongodb_port, persistent_db, collections)
    
        # Merge lookup district tables
    merge_lookup_district_tables(spark, vm_host, mongodb_port, persistent_db, formatted_db,
                                 "lookup_table_district", "Income_lookup_district", "Rebt_lookup_district") #Change names if needed

    # Merge lookup neighborhood tables
    merge_lookup_neighborhood_tables(spark, vm_host, mongodb_port, persistent_db, formatted_db,
                                     "lookup_table_neighborhood", "Income_lookup_neighborhood", "Rent_lookup_neigh")  #Change names if needed

if __name__ == "__main__":
    main()
