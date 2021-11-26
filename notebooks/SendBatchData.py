# Databricks notebook source
# MAGIC %md
# MAGIC #### Writing Batch Data to Kafka
# MAGIC The following is an example fo how to use spark api to write batch data (dataframe) to kafka

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
schema = StructType([ StructField("City", StringType(), True),StructField("State", StringType(), True), StructField("Population", LongType(), True),StructField("lat", DoubleType(), True) , StructField("lon", DoubleType(), True)])

# Similar to definition of staticInputDF above, just using `readStream` instead of `read`
inputDF = (
  spark
    .read                       
    .schema(schema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .option("header", True)
    .format("csv")
    .load("/FileStore/tables/cities")
)



# COMMAND ----------

rddWrite = inputDF.selectExpr("CAST(City as STRING) as key", "to_json(struct(*)) AS value").filter("City like '%Sa%'")
display(rddWrite)

# COMMAND ----------

TOPIC = "topic1"
BOOTSTRAP_SERVERS = "eh-ev-poc.servicebus.windows.net:9093"
EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://eh-ev-poc.servicebus.windows.net/;SharedAccessKeyName=poc-topics;SharedAccessKey=<access key>\";"


rddWrite.write.format("kafka") \
    .option("topic", TOPIC) \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS).option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.request.timeout.ms", 120000) \
    .option("kafka.sasl.jaas.config", EH_SASL) \
    .option("kafka.batch.size", 5000) \
    .option("client.id",'databricks-example-producer') \
    .option("checkpointLocation", "/checkpointing") \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Writing Stream Data to Kafka
# MAGIC The following is an example fo how to use spark api to write stream data (dataframe) to kafka

# COMMAND ----------

# Similar to definition of staticInputDF above, just using `readStream` instead of `read`
inputStreamDF = (
  spark
    .readStream                       
    .schema(schema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .option("header", True)
    .format("csv")
    .load("/FileStore/tables/cities")
)

# Is streaming ?
inputStreamDF.isStreaming

# COMMAND ----------

rddStreamWrite = inputStreamDF.selectExpr("CAST(City as STRING) as key", "to_json(struct(*)) AS value").filter("City like '%S%'")

# COMMAND ----------

rddStreamWrite.writeStream.format("kafka") \
    .option("topic", TOPIC) \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS).option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.request.timeout.ms", 120000) \
    .option("kafka.sasl.jaas.config", EH_SASL) \
    .option("kafka.batch.size", 1) \
    .option("client.id",'databricks-example-producer') \
    .option("checkpointLocation", "/cp4") \
    .start()

# COMMAND ----------


