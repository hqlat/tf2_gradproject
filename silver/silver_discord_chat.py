# Databricks notebook source
import pandas as pd
import datetime as dt
import pyspark
import pytz
from datetime import datetime
from dateutil import parser

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM adb_hibak.bronze_tf2_discord_general AS general
# MAGIC UNION ALL
# MAGIC SELECT * FROM adb_hibak.bronze_tf2_discord_mm_discussion AS mm_discuss
# MAGIC UNION ALL
# MAGIC SELECT * FROM adb_hibak.bronze_tf2_discord_tf2 AS tf2
# MAGIC UNION ALL
# MAGIC SELECT * FROM adb_hibak.bronze_tf2_discord_feedback_discussion AS bmod_feedback;
# MAGIC

# COMMAND ----------

# Converting PySpark df to Pandas df

discord_chats_df = _sqldf
discord_chats_df = discord_chats_df.toPandas()

# COMMAND ----------

discord_chats_df.info()

# COMMAND ----------

discord_chats_df = discord_chats_df[~discord_chats_df['Message'].str.contains(r'^<http.+>$')]

# COMMAND ----------

discord_chats_df.size

# COMMAND ----------

discord_chats_df = discord_chats_df[~discord_chats_df['Message'].str.contains(r'^[<>]|(http|https):\/\/\S+?$', regex=True)]

# COMMAND ----------

discord_chats_df.display()

# COMMAND ----------

#Reduce the DF with the features we ant
features = ["Author","Channel", "Timestamp","Message"]

discord_chats_df_trimmed = discord_chats_df[features].sort_values(by="Timestamp")


# COMMAND ----------

discord_chats_df_trimmed.display()

# COMMAND ----------

# Convert the Pandas df to a Spark df
spark_df_connections = spark.createDataFrame(discord_chats_df_trimmed)
spark_df_connections.display()

# COMMAND ----------

spark_df_connections.write.mode('overwrite').saveAsTable("adb_hibak.silver_tf2_discord_chat")

# COMMAND ----------

