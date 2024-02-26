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
# MAGIC SELECT * FROM adb_hibak.bronze_discord_lets_play_bmod AS bmod
# MAGIC UNION ALL
# MAGIC SELECT * FROM adb_hibak.bronze_discord_lets_play_mm AS mm
# MAGIC UNION ALL
# MAGIC SELECT * FROM adb_hibak.bronze_discord_server_seeding AS seeding;

# COMMAND ----------

# Converting PySpark df to Pandas df

discord_events_df = _sqldf
discord_events_df = discord_events_df.toPandas()

# COMMAND ----------

discord_events_df.info()

# COMMAND ----------

# Convert the 'timestamp' column to datetime
# discord_events_df['Timestamp'] = pd.to_datetime(discord_events_df['Timestamp'])

# # Convert the timestamp to Los Angeles time
# discord_events_df['la_time'] = discord_events_df['Timestamp'].dt.tz_convert('America/Los_Angeles')

# COMMAND ----------

# Create date from timestamp
# discord_events_df['Date'] = pd.to_datetime(discord_events_df['la_time']).dt.date
# discord_events_df['Time'] = discord_events_df['la_time'].dt.strftime('%H:%M')


# COMMAND ----------

discord_events_df.display()

# COMMAND ----------

#Reduce the DF with the features we ant
features = ["Author","Channel", "Timestamp"]

discord_events_df_trimmed = discord_events_df[features].sort_values(by="Timestamp")


# COMMAND ----------

discord_events_df_trimmed.display()

# COMMAND ----------

# Convert the Pandas df to a Spark df
spark_df_connections = spark.createDataFrame(discord_events_df_trimmed)
spark_df_connections.display()

# COMMAND ----------

spark_df_connections.write.mode('overwrite').saveAsTable("adb_hibak.silver_tf2_discord_events")