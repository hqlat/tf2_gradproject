# Databricks notebook source
import pandas as pd
import pyspark

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM adb_hibak.bronze_hlstats_players_history;

# COMMAND ----------

# Converting PySpark df to Pandas df

df_ph = _sqldf
df_ph = df_ph.toPandas()

# COMMAND ----------

df_ph.columns

# COMMAND ----------

# Select the columns we want.

features_ph = ['playerId', 'eventTime', 'connection_time', 'skill', 'skill_change']

df_ph = df_ph[features_ph]
df_ph

# COMMAND ----------

# Convert Pandas DF to a Spark DF. 
spark_df_players_connection_time = spark.createDataFrame(df_ph)
spark_df_players_connection_time.display()

# COMMAND ----------

# Convert the Spark DF to a table

spark_df_players_connection_time.write.mode('overwrite').saveAsTable("adb_hibak.silver_tf2_players_connection_time")
