'''
This PySpark script sets up a streaming application that consumes data from a
Kafka topic, processes it, and computes the daily running total of purchases
within each batch window.

The script performs the following tasks:
1. Loads environment variables from a .env file for configuration.
2. Creates a SparkSession with 'WARN' log level.
3. Defines a Kafka source for streaming and specifies options.
4. Specifies the schema for incoming Kafka messages and parses JSON values.
5. Converts the 'ts' column to a timestamp for time-based processing.
6. Creates a windowed DataFrame for daily running totals.
7. Defines a custom processing function for each batch.
8. Sets up a streaming query to display results in the console with a 2-minute interval.

This script continuously streams and processes data from the Kafka topic,
updating running totals in batch windows.
Typically used for real-time data processing and analytics.
'''
import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, from_unixtime, sum, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)

# Load environment variables
dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv('SPARK_MASTER_HOST_NAME')
spark_port = os.getenv('SPARK_MASTER_PORT')
kafka_host = os.getenv('KAFKA_HOST')
kafka_topic = os.getenv('KAFKA_TOPIC_NAME')

spark_host = f'spark://{spark_hostname}:{spark_port}'

# Set up SparkSession
spark = (
    SparkSession.builder
    .appName('DibimbingStreaming')
    .master(spark_host)
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

# Define the Kafka source for streaming with failOnDataLoss set to false
stream_df = (
    spark.readStream
    .format('kafka')
    .option('kafka.bootstrap.servers', f'{kafka_host}:9092')
    .option('subscribe', kafka_topic)
    .option('startingOffsets', 'latest')
    .option('failOnDataLoss', 'false')
    .load()
)

# Define the schema for the incoming Kafka messages
schema = StructType(
    [
        StructField('order_id', StringType(), True),
        StructField('customer_id', IntegerType(), True),
        StructField('furniture', StringType(), True),
        StructField('color', StringType(), True),
        StructField('price', IntegerType(), True),
        StructField('ts', StringType(), True),
    ]
)

# Parse the JSON value and convert the 'ts' column to a timestamp
parsed_df = (
    stream_df.selectExpr('CAST(value AS STRING)')
    .withColumn('value', from_json(col('value'), schema))
    .select(col('value.*'))
    .withColumn('ts', from_unixtime(col('ts')).cast(TimestampType()))
)

# Create a windowed DataFrame for daily total purchase within the batch
windowed_df = (
    parsed_df.withWatermark('ts', '15 minutes')
    .groupBy(window('ts', '1 day').alias('timestamp'))
    .agg(sum('price').alias('running_total'))
)

def process_batch(batch_df):
    """
    Process a batch of data in the specified DataFrame.

    This function selects the 'timestamp' and 'running_total' columns from the
    DataFrame, collects the data into a list, and returns it.

    Parameters:
    - batch_df (DataFrame): The DataFrame containing the batch of data to process.

    Returns:
    - list: A list of running total data for the batch.
    """
    # Select timestamp and running_total column
    running_total = batch_df.selectExpr('timestamp', 'running_total')

    # Collect the running total data of each batch into a list
    data_list = running_total.collect()

    return data_list


# Write the results to a custom processing function using foreachBatch
query = (
    windowed_df.writeStream
    .foreachBatch(process_batch)
    .format('console')
    .trigger(processingTime='2 minutes')
    .outputMode('update')
    .option('checkpointLocation', '/scripts/logs')
    .start()
)

query.awaitTermination()
