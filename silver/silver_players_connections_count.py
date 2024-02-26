# Databricks notebook source
import pandas as pd
import datetime as dt
import pyspark

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

# Remove time from datetime in eventTime column\n",
df_connect['eventTime'] = pd.to_datetime(df_connect['eventTime']).dt.date
df_disconnect['eventTime'] = pd.to_datetime(df_disconnect['eventTime']).dt.date

# COMMAND ----------

# Remove all rows with empty values in map.
df_connect = df_connect[df_connect['map'] != '']
df_disconnect = df_disconnect[df_disconnect['map'] != '']

# COMMAND ----------

df_connect.display()

# COMMAND ----------

df_disconnect.display()

# COMMAND ----------

# Find number of connections witht the unique combinations those three columns
df_connections_count = df_connect.groupby(['playerId', 'eventTime', 'serverId']).size().reset_index(name='connections_count')

# COMMAND ----------

# Find number of disconnections witht the unique combinations those three columns
df_disconnections_count = df_disconnect.groupby(['playerId', 'eventTime', 'serverId']).size().reset_index(name='disconnections_count')

# COMMAND ----------

df_connections_count.display()

# COMMAND ----------

df_disconnections_count.display()

# COMMAND ----------

# Merge connections and disconnections df
merged_df_connections = pd.merge(df_connections_count, df_disconnections_count, on=['eventTime', 'playerId', 'serverId'], how='outer')

# COMMAND ----------

merged_df_connections.display()

# COMMAND ----------

# Fill the NaN values with 0.
merged_df_connections = merged_df_connections.fillna(0)

# COMMAND ----------

# Convert the Pandas df to a Spark df
spark_df_connections = spark.createDataFrame(merged_df_connections)
spark_df_connections.display()

# COMMAND ----------

# Convert the Spark df to a table in the db.
spark_df_connections.write.mode('overwrite').saveAsTable("adb_hibak.gold_tf2_players_connections_count")