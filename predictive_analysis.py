# Importing the required libraries
from pyspark.sql import SparkSession
import logging
from mongo_utils import MongoDBUtils
from pyspark.sql.functions import col, expr
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator


#Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Function to create a Spark session
def create_spark_session(logger):
    try:
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("Unify Lookup District") \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        
    return None


# Function to move data from formatted zone to exploitation zone
def get_data_from_formatted_to_exploitation(self):
    # Read idealista_reconciled collection and select relevant columns
    idealista_df = MongoDBUtils.read_collection(
        self.logger,
        self.spark,
        self.vm_host,
        self.mongodb_port,
        self.formatted_db,
        "idealista_reconciled"
    ).select("_id", "size", "rooms", "bathrooms", "latitude", "longitude", "exterior", "floor", "has360",
        "has3DTour", "hasLift", "hasPlan", "hasStaging", "hasVideo","neighborhood_id", "numPhotos", "price") \
        .filter(col("municipality") == "Barcelona") \
        .filter(col("neighborhood_id").isNotNull())
    # Read income_reconciled collection and select relevant columns
    income_df = MongoDBUtils.read_collection(
        self.logger,
        self.spark,
        self.vm_host,
        self.mongodb_port,
        self.formatted_db,
        "income_reconciled"
    ).select("_id", "info.year", "info.RFD")
    # Read buildin_age_reconciled collection and select relevant columns
    buildin_age_df = MongoDBUtils.read_collection(
        self.logger,
        self.spark,
        self.vm_host,
        self.mongodb_port,
        self.formatted_db,
        "building_age_reconciled"
    ).select("_id", "info.year", "info.mean_age")
    # Join the three dataframes on district_id
    joined_df = idealista_df.join(
        income_df,
        idealista_df["neighborhood_id"] == income_df["_id"],
        "left"
    ).drop(income_df["_id"]).withColumnRenamed("year", "income_year")
    joined_df = joined_df.join(
        buildin_age_df,
        joined_df["neighborhood_id"] == buildin_age_df["_id"],
        "left"
    ).drop(buildin_age_df["_id"]).withColumnRenamed("year", "building_year")
    self.logger.info('Data sources joined successfully.')
    # Save the joined dataframe to a new collection in MongoDB
    MongoDBUtils.write_to_collection(
        self.logger,
        self.vm_host,
        self.mongodb_port,
        self.exploitation_db,
        "model_collection",
        joined_df
    )


def preprocess_and_train_model(self):
    df = MongoDBUtils.read_collection(
        self.logger,
        self.spark,
        self.vm_host,
        self.mongodb_port,
        self.exploitation_db,
        "model_collection"
    )

    # Drop unnecessary columns
    columns_to_drop = ['_id', 'income_year', 'building_year']
    df = df.drop(*columns_to_drop)

    # Calculate the mean of the 'mean_age' column
    df = df.withColumn('mean_age', expr(
        'aggregate(mean_age, CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) / size(mean_age)'))
    
    # Calculate the mean of the 'RFD' column
    df = df.withColumn('RFD', expr(
        'aggregate(RFD, CAST(0.0 AS DOUBLE), (acc, x) -> acc + x) / size(RFD)'))
    
    # Convert boolean columns to string and index them
    boolean_columns = ['exterior', 'has360', 'has3DTour', 'hasLift', 'hasPlan', 'hasStaging', 'hasVideo']
    for column_name in boolean_columns:
        df = df.withColumn(column_name, col(column_name).cast("string"))

    # Convert numeric columns to appropriate types
    numeric_cols = ["RFD", "bathrooms", "floor", "latitude", "longitude", "mean_age",
                    "numPhotos", "rooms", "size"]
    for column in numeric_cols:
        df = df.withColumn(column, df[column].cast("double"))

    # Convert string columns to numeric using StringIndexer
    string_cols = ["exterior", "has360", "has3DTour", "hasLift", "hasPlan",
                   "hasStaging", "hasVideo", "neighborhood_id"]
    indexers = [StringIndexer(inputCol=column, outputCol=column+"_index", handleInvalid="skip").fit(df) for column
                in string_cols]
    pipeline = Pipeline(stages=indexers)
    df = pipeline.fit(df).transform(df)
    df.dropna()

    # Assemble features into a single vector column
    feature_cols = numeric_cols + [column + "_index" for column in string_cols]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
    df = assembler.transform(df)

    # Split the data into training and testing sets
    (training_data, testing_data) = df.randomSplit([0.75, 0.25])

    # Create the Gradient-Boosted Trees Regressor model
    gbt = GBTRegressor(featuresCol="features", labelCol="price", seed=42)
    
    # Train the model
    model = gbt.fit(training_data)

    # Make predictions on the testing data
    predictions = model.transform(testing_data)

    # Evaluate the model
    ## Root Mean Squared Error (RMSE)
    rmse_evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
    rmse = rmse_evaluator.evaluate(predictions)

    ## Mean Absolute Error (MAE)
    mae_evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="mae")
    mae = mae_evaluator.evaluate(predictions)

    ## Mean Squared Error (MSE)
    mse_evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="mse")
    mse = mse_evaluator.evaluate(predictions)

    ## R-squared (R2)
    r2_evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="r2")
    r2 = r2_evaluator.evaluate(predictions)

    # Print the evaluation metrics
    self.logger.info(f"Root Mean Squared Error (RMSE): {rmse}")
    self.logger.info(f"Mean Absolute Error (MAE): {mae}", )
    self.logger.info(f"Mean Squared Error (MSE): {mse}")
    self.logger.info(f"R-squared (R2): {r2}")


    return model
