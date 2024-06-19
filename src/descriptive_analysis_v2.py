from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, avg, explode
import logging

#Configurate the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    try:
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("KPI Processor") \
            .config('spark.jars', 'C:/Users/pgome/Desktop/Master/2nd Semester/BDM-Big Data Management/P2_git/BDM-P2/postgresql-42.7.3.jar') \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def read_collection(spark, vm_host, mongodb_port, formatted_db, collection_name):
    return spark.read.format("mongo").option("uri", 
        f"mongodb://{vm_host}:{mongodb_port}/{formatted_db}.{collection_name}").load()

def calculate_kpis(spark, vm_host, mongodb_port, formatted_db):
    logger.info("Starting the calculation of the different KPIs.")
    #Read the data from the collections
    rent_df = read_collection(spark, vm_host, mongodb_port, formatted_db, "Rent_Idealista_reconciled") \
        .select("neighborhood_name", "price", "latitude", "longitude", "priceByArea")
    income_df = read_collection(spark, vm_host, mongodb_port, formatted_db, "Income_OpenBCN_reconciled") \
        .select("neighborhood_name", "info")
    density_df = read_collection(spark, vm_host, mongodb_port, formatted_db, "Density_OpenBCN_reconciled") \
        .select("neighborhood_name", "info")

    #Explode the info column to get the year and the KPI value in different columns
    income_df = income_df.withColumn("info", explode("info")).select("neighborhood_name", "info.year", "info.RFD")
    density_df = density_df.withColumn("info", explode("info")).select("neighborhood_name", "info.year", "info.density(inh/ha)")

    #Agregate the data to get the average values for each neighborhood in each KPI
    rent_avg_df = rent_df.groupBy("neighborhood_name").agg(avg("price").alias("avg_rent"), avg("latitude").alias("avg_latitude"), avg("longitude").alias("avg_longitude"), avg("priceByArea").alias("avg_price_by_area"))
    income_avg_df = income_df.groupBy("neighborhood_name").agg(avg("RFD").alias("avg_income"))
    density_avg_df = density_df.groupBy("neighborhood_name").agg(avg("density(inh/ha)").alias("avg_density"))

    #Join all the dataframes to get the final KPIs dataframe
    kpi_df = rent_avg_df.join(income_avg_df, on="neighborhood_name", how="left")
    kpi_df = kpi_df.join(density_avg_df, on="neighborhood_name", how="left")

    #Remove duplicate rows caused by the join
    kpi_df = kpi_df.drop(income_avg_df["neighborhood_name"]).drop(density_avg_df["neighborhood_name"])
    
    #If there are null values in the neighborhood_name column, we drop them
    kpi_df = kpi_df.dropna(subset=["neighborhood_name"])
    
    #We have some null Values, we fill them with random value. In a real case we should check why we have null values and impute them, as this is only to show how we made the implementation
    # we will impute them randomly.
    
    kpi_df = kpi_df.withColumn("avg_income", expr("IFNULL(avg_income, RAND() * 1000)"))
    kpi_df = kpi_df.withColumn("avg_density", expr("IFNULL(avg_density, RAND() * 1000)"))

    logger.info('KPIs calculated successfully.')

    return kpi_df

def save_to_postgres(df, postgres_url, table_name, postgres_user, postgres_password):
    properties = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver"

    }

    df.write.jdbc(url=postgres_url, table=table_name, mode="overwrite", properties=properties)
    logger.info(f'Data written to PostgreSQL table {table_name} successfully.')

def descriptive_analysis():
    #Initial Configuration
    logger = logging.getLogger("KPIProcessor")
    logging.basicConfig(level=logging.INFO)

    vm_host = "localhost"
    mongodb_port = "27017"
    formatted_db = "formatted_zone" 
    postgres_url = "jdbc:postgresql://postgresfib.fib.upc.edu:6433/ADSDBpablo.gomez.navarro"
    postgres_user = "pablo.gomez.navarro"
    postgres_password = "DB221100"

    #Create Spark Session
    spark = create_spark_session()

    #Fucntion to calcualte KPIs
    kpi_df = calculate_kpis(spark, vm_host, mongodb_port, formatted_db)

    #Take the calculated KPIs and save them to a Postgres table
    save_to_postgres(kpi_df, postgres_url, "kpis_table", postgres_user, postgres_password)
    logger.info("Test.")

# if __name__ == "__main__":
#     descriptive_analysis()
