# Databricks notebook source
import pandas as pd
import pyspark

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT * FROM adb_hibak.bronze_hlstats_PlayerNames;

# COMMAND ----------

# Converting PySpark df to Pandas df

df_players = _sqldf
df_players = df_players.toPandas()

# COMMAND ----------

df_players.columns

# COMMAND ----------

# Select the columns we want.
features = ["playerId","name"]

df_players = df_players[features]
df_players

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM adb_hibak.bronze_hlstats_playeruniqueids;

# COMMAND ----------

# Convert the PySpark DF to a Pandas DF.
df_unique_ids = _sqldf
df_unique_ids = df_unique_ids.toPandas()

# COMMAND ----------

# Select the columns we want in the DF
features_ids = ["playerId","uniqueId"]

df_unique_ids = df_unique_ids[features_ids]
df_unique_ids

# COMMAND ----------

# Merge the df_players with df_unique_ids to get all actual players.
merged_df = pd.merge(df_players, df_unique_ids, on='playerId', how='inner')
merged_df

# COMMAND ----------

# Remove all bots 
merged_df = merged_df[~merged_df['uniqueId'].str.startswith('BOT:')]

# COMMAND ----------

# Select the desired columns
merged_df = merged_df[['playerId', 'name']]
merged_df

# COMMAND ----------

# Convert Pandas DF to a Spark DF. 
spark_df_ph = spark.createDataFrame(merged_df)
spark_df_ph.display()

# COMMAND ----------

# Convert the Spark DF to a table
spark_df_ph.write.mode('overwrite').saveAsTable("adb_hibak.silver_tf2_players_names")

# COMMAND ----------


