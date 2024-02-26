# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM adb_hibak.bronze_tf2_group_organized_events;

# COMMAND ----------

group_events_df = _sqldf
group_events_df = group_events_df.toPandas()
group_events_df.display()

# COMMAND ----------

group_events_df["Timestamp"] = pd.to_datetime(group_events_df["eventTimeStart"]).dt.tz_localize('UTC').dt.tz_convert('Europe/Oslo')

# COMMAND ----------

group_events_df.display()

# COMMAND ----------

# creating a new column called "gameType" with default value "Unknown"
group_events_df["gameType"] = "Unknown"

# finding rows containing "balance" or "bmod" in eventTitle column
mask_bmod = group_events_df["eventTitle"].str.contains("balance|bmod", case=False)

# finding rows containing "Manned" or "Machines" or "robot" in eventTitle column
mask_mm = group_events_df["eventTitle"].str.contains("Manned|Machines|robot", case=False)

# updating values in gameType column based on the conditions
group_events_df.loc[mask_bmod, "gameType"] = "BMOD"
group_events_df.loc[mask_mm, "gameType"] = "MM"

# COMMAND ----------

features = ["Timestamp","gameType"]
reduced_df = group_events_df[features]

# COMMAND ----------

reduced_df.display()

# COMMAND ----------

sparkdf = spark.createDataFrame(reduced_df)

sparkdf.write.mode('overwrite').saveAsTable("adb_hibak.silver_tf2_group_organized_events")