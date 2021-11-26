# Databricks notebook source
readConfig = {
  "Endpoint" : "https://kikedb-poc.documents.azure.com:443/",
  "Masterkey" : "<master-key>",
  "Database" : "sample",
  "Collection" : "sample01",
  "query_custom" : "SELECT c.id, c.name, c.cc, c.location FROM c"
}

# COMMAND ----------

# MAGIC %md
# MAGIC The cosmosdb library (https://github.com/Azure/azure-cosmosdb-spark) must be previously installed on the cluster before executing the following code

# COMMAND ----------

data = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**readConfig).load()
data.count()

# COMMAND ----------

display(data)

# COMMAND ----------

data.createOrReplaceTempView("myview")

# COMMAND ----------

from pyspark.sql import *


someData = spark.createDataFrame([Row(id='000001', city='Eindhoven')])
someData.createOrReplaceTempView("someData")
display(someData)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sampleupdate
# MAGIC AS 
# MAGIC  SELECT mv.*, sd.city 
# MAGIC FROM myview mv
# MAGIC INNER JOIN someData sd
# MAGIC  ON mv.id = sd.id

# COMMAND ----------

updatedData = sqlContext.table("sampleupdate")
display(updatedData)

# COMMAND ----------

writeConfig = {
  "Endpoint" : "https://kikedb-poc.documents.azure.com:443/",
  "Masterkey" : "<master-key>",
  "Database" : "sample",
  "Collection" : "sample01",
  "Upsert": True
}


# COMMAND ----------

updatedData.write.format("com.microsoft.azure.cosmosdb.spark").mode("overwrite").options(**readConfig).save()

# COMMAND ----------


