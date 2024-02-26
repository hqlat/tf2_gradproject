# Databricks notebook source
import pandas as pd
import pyspark

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM adb_hibak.silver_tf2_players_names;

# COMMAND ----------

# Converting PySpark df to Pandas df

df_ph = _sqldf
df_ph = df_ph.toPandas()

# COMMAND ----------

# Sort the DataFrame by 'playerId' and the index in descending order
# df_ph = df_ph.sort_values(by=['playerId', df_ph.index], ascending=[True, False])

# Use drop_duplicates to keep only the last occurrence of each player ID
df_ph_result = df_ph.drop_duplicates(subset='playerId', keep='first')

# Reset the index if needed
df_ph_result = df_ph_result.reset_index(drop=True)

# Display the result
print(df_ph_result)

# COMMAND ----------

# Select the columns we want.

features_ph = ['playerId', 'name']

df_ph = df_ph_result[features_ph]
df_ph

# COMMAND ----------

# Convert Pandas DF to a Spark DF. 
spark_df_player_names = spark.createDataFrame(df_ph)
spark_df_player_names.display()

# COMMAND ----------

# Convert the Spark DF to a table

spark_df_player_names.write.mode('overwrite').saveAsTable("adb_hibak.gold_tf2_player_names")

# COMMAND ----------


