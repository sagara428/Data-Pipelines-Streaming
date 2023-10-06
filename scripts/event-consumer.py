import pyspark
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Load environment variables
dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv('SPARK_MASTER_HOST_NAME')
spark_port = os.getenv('SPARK_MASTER_PORT')
kafka_host = os.getenv('KAFKA_HOST')
kafka_topic = os.getenv('KAFKA_TOPIC_NAME')

spark_host = f'spark://{spark_hostname}:{spark_port}'

# Set up SparkSession
spark = SparkSession.builder.appName('DibimbingStreaming').master(spark_host).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Define the Kafka source for streaming with failOnDataLoss set to false
stream_df = (
    spark.readStream.format('kafka')
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
    .withColumn('value', from_json('value', schema))
    .select('value.*')
    .withColumn('ts', from_unixtime('ts').cast('timestamp'))
)

# Create a windowed DataFrame for daily total purchase within the batch
windowed_df = (
    parsed_df.withWatermark('ts', '15 minutes')
    .groupBy(window('ts', '1 day').alias('timestamp'))
    .agg(sum('price').alias('daily_total'))
)


# Define a function to process each batch of data
def process_batch(batch_df, batch_id):
    # Calculate the running total for this batch
    running_total = batch_df.selectExpr('timestamp', 'sum(daily_total) as running_total').collect()
    
    # Collect the data into a list
    data_list = running_total
    
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