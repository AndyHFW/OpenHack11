# Databricks notebook source
# MAGIC %scala
# MAGIC dbutils.fs.mount(
# MAGIC   source = "wasbs://challenge2@messagesiot.blob.core.windows.net/",
# MAGIC   mountPoint = "/mnt/challenge2",
# MAGIC   extraConfigs = Map("fs.azure.account.key.messagesiot.blob.core.windows.net" -> "P+7VO33kfq90BVfZ3yn6YpBBoolMZfdwCnRwMBYXPQGskAkw2kKQNRTnu//3k7/HxwLBBhPQgdbq70cfeI4Bhg=="))

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.messagesiot.blob.core.windows.net",
  "P+7VO33kfq90BVfZ3yn6YpBBoolMZfdwCnRwMBYXPQGskAkw2kKQNRTnu//3k7/HxwLBBhPQgdbq70cfeI4Bhg==")
dbutils.fs.put("/mnt/challenge2/test", "hello world again", True)
dbutils.fs.ls("wasbs://challenge2@messagesiot.blob.core.windows.net/")


# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.eventhubs._
# MAGIC import org.apache.spark.sql.types._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.streaming.Trigger.ProcessingTime
# MAGIC 
# MAGIC 
# MAGIC val connectionString =  ConnectionStringBuilder("Endpoint=sb://iothub-ns-torontoope-1352814-3aa2009056.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=4lT9BzVZzo0U/GW74DTHd2A1cWcrOFNEg9qXp0KNLQ8=;EntityPath=torontoopenhack11").setEventHubName("torontoopenhack11").build
# MAGIC 
# MAGIC val customEventhubParameters = EventHubsConf(connectionString).setConsumerGroup("eventhubs")
# MAGIC   //.setMaxEventsPerTrigger(5)
# MAGIC val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()
# MAGIC 
# MAGIC var streamingSelectDF = 
# MAGIC   incomingStream
# MAGIC    .select($"body".cast(StringType), get_json_object(($"body").cast("string"), "$.correlationId").alias("correlationId"),
# MAGIC            get_json_object(($"body").cast("string"), "$.deviceTime").alias("deviceTime"), date_format(get_json_object(($"body").cast("string"), "$.deviceTime"), "yyyy").alias("year"), date_format(get_json_object(($"body").cast("string"), "$.deviceTime"), "MM").alias("month"), date_format(get_json_object(($"body").cast("string"), "$.deviceTime"), "dd").alias("day"))
# MAGIC 
# MAGIC streamingSelectDF.writeStream.outputMode("append").format("parquet").partitionBy("year","month","day", "correlationId")
# MAGIC .option("path",s"/mnt/challenge2/messages")
# MAGIC .option("checkpointLocation",s"/mnt/challenge2/check")
# MAGIC .trigger(ProcessingTime("25 seconds"))
# MAGIC .start().awaitTermination()
# MAGIC 
# MAGIC /* val messages =
# MAGIC       incomingStream
# MAGIC       .withColumn("Offset", $"offset".cast(LongType))
# MAGIC       .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
# MAGIC       .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
# MAGIC       .withColumn("Body", $"body".cast(StringType))
# MAGIC       .select("Offset", "Time (readable)", "Timestamp", "Body")
# MAGIC */
# MAGIC //messages.printSchema
# MAGIC 
# MAGIC // Sending the incoming stream into the console.
# MAGIC // Data comes in batches!
# MAGIC //messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
