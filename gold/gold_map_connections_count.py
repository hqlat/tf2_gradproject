# Databricks notebook source
import pandas as pd
import datetime as dt
import pyspark
from pyspark.sql.functions import to_date, col, date_format

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM adb_hibak.silver_tf2_connect;

# COMMAND ----------

# Converting PySpark df to Pandas df

df_connect = _sqldf
df_connect = df_connect.toPandas()

# COMMAND ----------

df_connect.info()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM adb_hibak.silver_tf2_disconnect;

# COMMAND ----------

# Converting PySpark df to Pandas df
df_disconnect = _sqldf
df_disconnect = df_disconnect.toPandas()

# COMMAND ----------

df_disconnect.info()

# COMMAND ----------

# Check the number of connections per map.
df_connect.groupby('map').size()

# COMMAND ----------

# Check the number of disconnections per map.
df_disconnect.groupby('map').size()

# COMMAND ----------

# Remove time from datetime in eventTime column
df_connect['event_time'] = pd.to_datetime(df_connect['event_time']).dt.date
df_disconnect['event_time'] = pd.to_datetime(df_disconnect['event_time']).dt.date

# COMMAND ----------

# Find number of connections witht the unique combinations those three columns and get a count per map
df_connections_count_maps = df_connect.groupby(['map', 'event_time', 'server_id']).size().reset_index(name='connections_count')

# COMMAND ----------

# Find number of disconnections witht the unique combinations those three columns and get a count per map
df_disconnections_count_maps = df_disconnect.groupby(['map', 'event_time', 'server_id']).size().reset_index(name='disconnections_count')

# COMMAND ----------

# Merge connections and disconnections df
merged_connections_maps = pd.merge(df_connections_count_maps, df_disconnections_count_maps, on=['event_time', 'map', 'server_id'], how='outer')

# COMMAND ----------

merged_connections_maps.display()

# COMMAND ----------

# Fill the NaN values with 0.
merged_connections_maps = merged_connections_maps.fillna(0)

# COMMAND ----------

merged_connections_maps.info()

# COMMAND ----------

df_connections_count_game_mode = df_connect.groupby(['event_time', 'server_id', 'game_mode']).size().reset_index(name='connections_count')

# COMMAND ----------

df_disconnections_count_game_mode = df_disconnect.groupby(['event_time', 'server_id', 'game_mode']).size().reset_index(name='disconnections_count')

# COMMAND ----------

merged_connections_game_mode = pd.merge(df_connections_count_game_mode, df_disconnections_count_game_mode, on=['event_time', 'game_mode', 'server_id'], how='outer')

# COMMAND ----------

merged_connections_game_mode.display()

# COMMAND ----------

# Fill the NaN values with 0.
merged_connections_game_mode = merged_connections_game_mode.fillna(0)

# COMMAND ----------

# Convert to 'date' column to datetime
merged_connections_maps['event_time'] = pd.to_datetime(merged_connections_maps['event_time'])
merged_connections_game_mode['event_time'] = pd.to_datetime(merged_connections_game_mode['event_time'])

# COMMAND ----------

# # Extract the weekday from event_time
merged_connections_maps['weekday'] = merged_connections_maps['event_time'].dt.dayofweek
merged_connections_game_mode['weekday'] = merged_connections_game_mode['event_time'].dt.dayofweek

# COMMAND ----------

# Add a column checking whether it is the weekend.
merged_connections_maps['is_weekend'] = merged_connections_maps['event_time'].dt.dayofweek.isin([5, 6]).astype(int)
merged_connections_game_mode['is_weekend'] = merged_connections_game_mode['event_time'].dt.dayofweek.isin([5, 6]).astype(int)

# COMMAND ----------

merged_connections_maps.head()

# COMMAND ----------

merged_connections_maps.to_csv('merged_connections_maps.csv', index=False)

# COMMAND ----------

merged_connections_game_mode.display()

# COMMAND ----------

# Convert the Pandas df to a Spark df
spark_df_connections = spark.createDataFrame(merged_connections_maps)
spark_df_connections.display()

# COMMAND ----------

# Convert the Pandas df to a Spark df
spark_df_connections_game_mode = spark.createDataFrame(merged_connections_game_mode)
spark_df_connections_game_mode.display()

# COMMAND ----------

# Convert the 'date' column to a formatted string without the timestamp
date_format_pattern = "yyyy-MM-dd"  # Specify the desired format
spark_df_connections = spark_df_connections.withColumn("event_time", date_format(col("event_time"), date_format_pattern))

# COMMAND ----------

# Convert the 'date' column to a formatted string without the timestamp
date_format_pattern = "yyyy-MM-dd"  # Specify the desired format
spark_df_connections_game_mode = spark_df_connections_game_mode.withColumn("event_time", date_format(col("event_time"), date_format_pattern))

# COMMAND ----------

spark_df_connections.display()

# COMMAND ----------

spark_df_connections_game_mode.display()

# COMMAND ----------

# Convert the Spark df to a table in the db.
spark_df_connections.write.mode('overwrite').saveAsTable("adb_hibak.gold_tf2_map_connections_count")

# COMMAND ----------

# Convert the Spark df to a table in the db.
spark_df_connections_game_mode.write.mode('overwrite').saveAsTable("adb_hibak.gold_tf2_game_mode_connections_count")
