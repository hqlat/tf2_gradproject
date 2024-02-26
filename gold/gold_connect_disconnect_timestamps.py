# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM adb_hibak.silver_tf2_disconnect as disconnect;
# MAGIC SELECT * FROM adb_hibak.silver_tf2_connect as disconnect;

# COMMAND ----------

disconnect_df = spark.sql("SELECT *, 'Disconnect' as connect_type FROM adb_hibak.silver_tf2_disconnect as disconnect")
connect_df = spark.sql("SELECT *, 'Connect' as connect_type FROM adb_hibak.silver_tf2_connect as connect")
#Connect type 1 = Connect, 0 = Disconnect
combined_df = disconnect_df.unionAll(connect_df)

# COMMAND ----------

combined_df.display()

# COMMAND ----------

combined_df = combined_df.toPandas()

# COMMAND ----------

# Remove rows where 'map' is empty
combined_df = combined_df.dropna(subset=['map'], how='any')

# COMMAND ----------

spark_df_connect_disconnect = spark.createDataFrame(combined_df)

# COMMAND ----------

spark_df_connect_disconnect.write.mode('overwrite').saveAsTable("adb_hibak.gold_tf2_connect_disconnect")

# COMMAND ----------

