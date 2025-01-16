# Databricks notebook source
# MAGIC %md
# MAGIC # Real-time Data Processing with Azure Databricks (and Event Hubs)
# MAGIC
# MAGIC This notebook demonstrates the below architecture to build real-time data pipelines.
# MAGIC ![Solution Architecture](https://raw.githubusercontent.com/malvik01/Real-Time-Streaming-with-Azure-Databricks/main/Azure%20Solution%20Architecture.png)
# MAGIC
# MAGIC
# MAGIC - Data Sources: Streaming data from IoT devices or social media feeds. (Simulated in Event Hubs)
# MAGIC - Ingestion: Azure Event Hubs for capturing real-time data.
# MAGIC - Processing: Azure Databricks for stream processing using Structured Streaming.
# MAGIC - Storage: Processed data stored Azure Data Lake (Delta Format).
# MAGIC - Visualisation: Data visualized using Power BI.
# MAGIC
# MAGIC
# MAGIC ### Azure Services Required
# MAGIC - Databricks Workspace (Unity Catalog enabled)
# MAGIC - Azure Data Lake Storage (Premium)
# MAGIC - Azure Event Hub (Basic Tier)
# MAGIC
# MAGIC ### Azure Databricks Configuration Required
# MAGIC - Single Node Compute Cluster: `12.2 LTS (includes Apache Spark 3.3.2, Scala 2.12)`
# MAGIC - Maven Library installed on Compute Cluster: `com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22`

# COMMAND ----------

# MAGIC %md
# MAGIC Importing the libraries.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC The code block below creates the catalog and schemas for our solution. 
# MAGIC
# MAGIC The approach utilises a multi-hop data storage architecture (medallion), consisting of bronze, silver, and gold schemas within a 'streaming' catalog. 

# COMMAND ----------

try:
    spark.sql("create catalog streaming;")
except:
    print('check if catalog already exists')

try:
    spark.sql("create schema streaming.bronze;")
except:
    print('check if bronze schema already exists')

try:
    spark.sql("create schema streaming.silver")
except:
    print('check if silver schema already exists')

try:
    spark.sql("create schema streaming.gold;")
except:
    print('check if gold schema already exists')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC Set up Azure Event hubs connection string.

# COMMAND ----------

# Config
# Replace with your Event Hub namespace, name, and key
connectionString = "input connection strings here(access keys created in evenhub)"
eventHubName = "name of event hub

ehConf = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
  'eventhubs.eventHubName': eventHubName
}

# COMMAND ----------

# MAGIC %md
# MAGIC Reading and writing the stream to the bronze layer.

# COMMAND ----------

# Reading stream: Load data from Azure Event Hub into DataFrame 'df' using the previously configured settings
df = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load() \

# Displaying stream: Show the incoming streaming data for visualization and debugging purposes
df.display()

# Writing stream: Persist the streaming data to a Delta table 'streaming.bronze.weather' in 'append' mode with checkpointing
df.writeStream\
    .option("checkpointLocation", "/mnt/streaming/bronze/weather")\
    .outputMode("append")\
    .format("delta")\
    .toTable("streaming.bronze.weather")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM streaming.bronze.weather

# COMMAND ----------

# MAGIC %md
# MAGIC #### Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC Defining the schema for the JSON object.

# COMMAND ----------

# Defining the schema for the JSON object

json_schema = StructType([
    StructField("temperature", IntegerType()),
    StructField("humidity", IntegerType()),
    StructField("windSpeed", IntegerType()),
    StructField("windDirection", StringType()),
    StructField("precipitation", IntegerType()),
    StructField("conditions", StringType())
])

# COMMAND ----------

# MAGIC %md
# MAGIC Reading, transforming and writing the stream from the bronze to the silver layer.

# COMMAND ----------

##  test cell using regular read
temp_df = spark.read.table('streaming.bronze.weather')\
    .withColumn('body', col('body').cast('string'))\
      .withColumn("body",from_json(col("body"), json_schema))\
        .select("body.temperature", "body.humidity", "body.windSpeed", "body.windDirection", "body.precipitation", "body.conditions", col("enqueuedTime").alias('timestamp'))

temp_df.display()

# COMMAND ----------

# Reading and Transforming: Load streaming data from the 'streaming.bronze.weather' Delta table, cast 'body' to string, parse JSON, and select specific fields
df = spark.readStream\
    .format("delta")\
    .table("streaming.bronze.weather")\
    .withColumn("body", col("body").cast("string"))\
    .withColumn("body",from_json(col("body"), json_schema))\
    .select("body.temperature", "body.humidity", "body.windSpeed", "body.windDirection", "body.precipitation", "body.conditions", col("enqueuedTime").alias('timestamp'))

# Displaying stream: Visualize the transformed data in the DataFrame for verification and analysis
df.display()

# Writing stream: Save the transformed data to the 'streaming.silver.weather' Delta table in 'append' mode with checkpointing for data reliability
df.writeStream\
    .option("checkpointLocation", "/mnt/streaming/silver/weather")\
    .outputMode("append")\
    .format("delta")\
    .toTable("streaming.silver.weather")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM streaming.silver.weather

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC Reading, aggregating and writing the stream from the silver to the gold layer.

# COMMAND ----------

# Aggregating Stream: Read from 'streaming.silver.weather', apply watermarking and windowing, and calculate average weather metrics
df = spark.readStream\
    .format("delta")\
    .table("streaming.silver.weather")\
    .withWatermark("timestamp", "5 minutes") \
    .groupBy(window("timestamp", "5 minutes")) \
    .agg(avg("temperature").alias('temperature'), avg("humidity").alias('humidity'), avg("windSpeed").alias('windSpeed'), avg("precipitation").alias('precipitation'))\
	.select('window.start', 'window.end', 'temperature', 'humidity', 'windSpeed', 'precipitation')

# Displaying Aggregated Stream: Visualize aggregated data for insights into weather trends
df.display()

# Writing Aggregated Stream: Store the aggregated data in 'streaming.gold.weather_aggregated' with checkpointing for data integrity
df.writeStream\
    .option("checkpointLocation", "/mnt/streaming/gold/weather_summary")\
    .outputMode("append")\
    .format("delta")\
    .toTable("streaming.gold.weather_summary")