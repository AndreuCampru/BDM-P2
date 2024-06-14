import logging
from data_formatters import create_spark_session, drop_duplicates_and_save_new_collection

#Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Main function
def main():
    #Different host and port for the MongoDB
    vm_host = 'localhost'
    mongodb_port = 27017
    db_name = 'PersistentZone'
    collections = ['OpenBCN']  #List of collections, maybe we can change this later

    #Create a Spark session
    spark = create_spark_session()

    #Drop duplicates and save new collection
    drop_duplicates_and_save_new_collection(spark, vm_host, mongodb_port, db_name, collections)

if __name__ == "__main__":
    main()
